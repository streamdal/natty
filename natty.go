package natty

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
)

const (
	DefaultMaxMsgs       = 10_000
	DefaultFetchSize     = 100
	DefaultFetchTimeout  = time.Second * 1
	DefaultDeliverPolicy = nats.DeliverLastPolicy
)

type INatty interface {
	Consume(ctx context.Context, cb func(msg *nats.Msg) error) error
	Publish(ctx context.Context, subject string, data []byte) error
}

type Config struct {
	// NatsURL defines the NATS urls the library will attempt to connect to. If
	// first URL fails, we will try to connect to the next one. Only fail if all
	// URLs fail.
	NatsURL []string

	// StreamName is the name of the stream the library will attempt to create.
	// Stream creation will NOOP if the stream already exists.
	StreamName string

	// StreamSubjects defines the subjects that the stream will listen to.
	StreamSubjects []string

	// ConsumerName defines the name of the consumer the library will create.
	// NOTE: The library *always* creates durable consumers.
	ConsumerName string

	// MaxMsgs defines the maximum number of messages a stream will contain.
	MaxMsgs int64

	// FetchSize defines the number of messages to fetch from the stream during
	// a single Fetch() call.
	FetchSize int

	// FetchTimeout defines how long a Fetch() call will wait to attempt to reach
	// defined FetchSize before continuing.
	FetchTimeout time.Duration

	// DeliverPolicy defines the policy the library will use to deliver messages.
	// Default: DeliverLastPolicy which will deliver from the last message that
	// the consumer has seen.
	DeliverPolicy nats.DeliverPolicy

	// Logger allows you to inject a logger into the library. Optional.
	Logger Logger

	// ConsumerLooper allows you to inject a looper into the library. Optional.
	ConsumerLooper director.Looper
}

type Natty struct {
	js             nats.JetStreamContext
	consumerLooper director.Looper
	config         *Config
	log            Logger
}

func New(cfg *Config) (*Natty, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	var connected bool
	var nc *nats.Conn
	var err error

	// Attempt to connect
	for _, address := range cfg.NatsURL {
		nc, err = nats.Connect(address)
		if err != nil {
			continue
		}

		connected = true
	}

	if !connected {
		return nil, errors.Wrap(err, "failed to connect to NATS")
	}

	// Create js context
	js, err := nc.JetStream()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create jetstream context")
	}

	// Create stream
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     cfg.StreamName,
		Subjects: cfg.StreamSubjects,
		MaxMsgs:  cfg.MaxMsgs,
	}); err != nil {
		return nil, errors.Wrap(err, "unable to create stream")
	}

	// Create consumer
	if _, err := js.AddConsumer(cfg.StreamName, &nats.ConsumerConfig{
		Durable:   cfg.ConsumerName,
		AckPolicy: nats.AckExplicitPolicy,
	}); err != nil {
		return nil, errors.Wrap(err, "unable to create consumer")
	}

	n := &Natty{
		js:     js,
		config: cfg,
	}

	if cfg.ConsumerLooper == nil {
		n.consumerLooper = director.NewFreeLooper(director.FOREVER, make(chan error, 1))
	}

	if cfg.Logger == nil {
		n.log = &NoOpLogger{}
	}

	return n, nil
}

// Consume will create a durable consumer and consume messages from the configured stream
func (n *Natty) Consume(ctx context.Context, subj string, errorCh chan error, f func(msg *nats.Msg) error) error {
	sub, err := n.js.PullSubscribe(subj, n.config.ConsumerName)
	if err != nil {
		return errors.Wrap(err, "unable to create subscription")
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			n.log.Errorf("unable to unsubscribe from (stream: '%s', subj: '%s'): %s",
				n.config.StreamName, subj, err)
		}
	}()

	var quit bool

	n.consumerLooper.Loop(func() error {
		// This is needed to prevent context flood in case .Quit() wasn't picked
		// up quickly enough by director
		if quit {
			time.Sleep(25 * time.Millisecond)
			return nil
		}

		msgs, err := sub.Fetch(n.config.FetchSize, nats.Context(ctx))
		if err != nil {
			if err == context.Canceled {
				n.log.Debugf("context canceled (stream: %s, subj: %s)",
					n.config.StreamName, subj)

				n.consumerLooper.Quit()
				quit = true

				return nil
			}

			if err == context.DeadlineExceeded {
				// No problem, context timed out - try again
				return nil
			}

			n.report(errorCh, fmt.Errorf("unable to fetch messages from (stream: '%s', subj: '%s'): %s",
				n.config.StreamName, subj, err))

			return nil
		}

		for _, v := range msgs {
			if err := f(v); err != nil {
				n.report(errorCh, fmt.Errorf("callback func failed during message processing (stream: '%s', subj: '%s', msg: '%s'): %s",
					n.config.StreamName, subj, v.Data, err))
			}
		}

		return nil
	})

	n.log.Debugf("consumer exiting (stream: %s, subj: %s)", n.config.StreamName, subj)

	return nil
}

func (n *Natty) Publish(ctx context.Context, subj string, msg []byte) error {
	if _, err := n.js.Publish(subj, msg, nats.Context(ctx)); err != nil {
		return errors.Wrap(err, "unable to publish message")
	}

	return nil
}

func (n *Natty) report(errorCh chan error, err error) {
	if errorCh != nil {
		// Write the err in a goroutine to avoid block in case chan is full
		go func() {
			errorCh <- err
		}()
	}

	n.log.Error(err)
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("config cannot be nil")
	}

	if len(cfg.NatsURL) == 0 {
		return errors.New("NatsURL cannot be empty")
	}

	if cfg.StreamName == "" {
		return errors.New("StreamName cannot be empty")
	}

	if len(cfg.StreamSubjects) == 0 {
		return errors.New("StreamSubjects cannot be empty")
	}

	if cfg.MaxMsgs == 0 {
		cfg.MaxMsgs = DefaultMaxMsgs
	}

	if cfg.FetchSize == 0 {
		cfg.FetchSize = DefaultFetchSize
	}

	if cfg.FetchTimeout == 0 {
		cfg.FetchTimeout = DefaultFetchTimeout
	}

	if cfg.DeliverPolicy == 0 {
		cfg.DeliverPolicy = DefaultDeliverPolicy
	}

	return nil
}
