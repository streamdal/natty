package natty

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	uuid "github.com/satori/go.uuid"
)

const (
	DefaultMaxMsgs       = 10_000
	DefaultFetchSize     = 100
	DefaultFetchTimeout  = time.Second * 1
	DefaultDeliverPolicy = nats.DeliverLastPolicy
	DefaultSubBatchSize  = 256
)

var (
	ErrEmptyStreamName   = errors.New("StreamName cannot be empty")
	ErrEmptyConsumerName = errors.New("ConsumerName cannot be empty")
	ErrEmptySubject      = errors.New("Subject cannot be empty")
)

type Mode int

type INatty interface {
	// Consume subscribes to given subject and executes callback every time a
	// message is received. This is a blocking call; cancellation should be
	// performed via the context.
	Consume(ctx context.Context, subj, streamName, consumerName string, errorCh chan error, cb func(ctx context.Context, msg *nats.Msg) error) error

	// Publish publishes a single message with the given subject
	Publish(ctx context.Context, subject string, data []byte) error

	// NATS key/value Get/Put/Delete/Update functionality operates on "buckets"
	// that are exposed via a 'KeyValue' instance. To simplify our interface,
	// our Put method will automatically create the bucket if it does not already
	// exist. Get() and Delete() will not automatically create a bucket.
	//
	// If your functionality is creating many ephemeral buckets, you may want to
	// delete the bucket after you are done via DeleteBucket().

	// Get will fetch the value for a given bucket and key. Will NOT auto-create
	// bucket if it does not exist.
	Get(ctx context.Context, bucket string, key string) ([]byte, error)

	// Put will put a new value for a given bucket and key. Will auto-create
	// the bucket if it does not already exist.
	Put(ctx context.Context, bucket string, key string, data []byte, ttl ...time.Duration) error

	// Delete will delete a key from a given bucket. Will no-op if the bucket
	// or key does not exist.
	Delete(ctx context.Context, bucket string, key string) error

	// CreateStream creates a new stream if it does not exist
	CreateStream(name string) error

	// DeleteStream deletes an existing stream
	DeleteStream(name string) error

	// CreateConsumer creates a new consumer if it does not exist
	CreateConsumer(streamName, consumerName string) error

	// DeleteConsumer deletes an existing consumer
	DeleteConsumer(consumerName, streamName string) error
}

type Config struct {
	// NatsURL defines the NATS urls the library will attempt to connect to. Iff
	// first URL fails, we will try to connect to the next one. Only fail if all
	// URLs fail.
	NatsURL []string

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

	// Whether to use TLS
	UseTLS bool

	// TLS CA certificate file
	TLSCACertFile string

	// TLS client certificate file
	TLSClientCertFile string

	// TLS client key file
	TLSClientKeyFile string

	// Do not perform server certificate checks
	TLSSkipVerify bool
}

// ConsumerConfig is used to pass configuration options to Consume()
type ConsumerConfig struct {
	// Subject is the subject to consume off of a stream
	Subject string

	// StreamName is the name of JS stream to consume from.
	// This should first be created with CreateStream()
	StreamName string

	// ConsumerName is the consumer that was made with CreateConsumer()
	ConsumerName string

	// Looper is optional, if none is provided, one will be created
	Looper director.Looper

	// ErrorCh is used to retrieve any errors returned in the consumer looper
	ErrorCh chan error
}

type Publisher struct {
	ID         string
	QueueMutex *sync.RWMutex
	Queue      []*message
	nc         *nats.Conn
	looper     director.Looper
	log        Logger
}

// message is a convenience struct to hold message data for a batch
type message struct {
	Subject string
	Value   []byte
}

type Natty struct {
	nc             *nats.Conn
	js             nats.JetStreamContext
	consumerLooper director.Looper
	config         *Config
	kvMap          *KeyValueMap
	kvMutex        *sync.RWMutex
	publisherMutex *sync.RWMutex
	publisherMap   map[string]*Publisher
	log            Logger
}

func New(cfg *Config) (*Natty, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	var connected bool
	var nc *nats.Conn
	var err error
	var tlsConfig *tls.Config

	if cfg.UseTLS {
		tlsConfig, err = GenerateTLSConfig(cfg.TLSCACertFile, cfg.TLSClientCertFile, cfg.TLSClientKeyFile, cfg.TLSSkipVerify)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create TLS config")
		}
	}

	// Attempt to connect
	for _, address := range cfg.NatsURL {
		if cfg.UseTLS {
			nc, err = nats.Connect(address, nats.Secure(tlsConfig))
		} else {
			nc, err = nats.Connect(address)
		}

		if err != nil {
			fmt.Printf("unable to connect to '%s': %s\n", address, err)

			continue
		}

		connected = true
		break
	}

	if !connected {
		return nil, errors.Wrap(err, "failed to connect to NATS")
	}

	// Create js context
	js, err := nc.JetStream()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create jetstream context")
	}

	n := &Natty{
		nc:     nc,
		js:     js,
		config: cfg,
		kvMap: &KeyValueMap{
			rwMutex: &sync.RWMutex{},
			kvMap:   make(map[string]nats.KeyValue),
		},
		publisherMutex: &sync.RWMutex{},
		publisherMap:   make(map[string]*Publisher),
	}

	// Inject logger (if provided)
	n.log = cfg.Logger

	if n.log == nil {
		n.log = &NoOpLogger{}
	}

	return n, nil
}

func (n *Natty) DeleteStream(name string) error {
	return n.js.DeleteStream(name)
}

func (n *Natty) CreateStream(name string) error {
	// Check if stream exists
	_, err := n.js.StreamInfo(name)
	if err == nil {
		// We have a stream already, nothing else to do
		return nil
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		return errors.Wrap(err, "unable to create stream")
	}

	_, err = n.js.AddStream(&nats.StreamConfig{
		Name:      name,
		Subjects:  []string{name},
		Retention: nats.LimitsPolicy,   // Limit to age
		MaxAge:    time.Hour * 24 * 30, // 30 days max retention
		Storage:   nats.FileStorage,    // Store on disk

	})
	if err != nil {
		return errors.Wrap(err, "unable to create stream")
	}

	return nil
}

func GenerateTLSConfig(caCertFile, clientKeyFile, clientCertFile string, tlsSkipVerify bool) (*tls.Config, error) {
	if caCertFile == "" && clientKeyFile == "" && clientCertFile == "" {
		return &tls.Config{
			InsecureSkipVerify: tlsSkipVerify,
		}, nil
	}

	var certpool *x509.CertPool

	if caCertFile != "" {
		certpool = x509.NewCertPool()

		pemCerts, err := ioutil.ReadFile(caCertFile)
		if err == nil {
			certpool.AppendCertsFromPEM(pemCerts)
		}
	}

	var cert tls.Certificate

	if clientKeyFile != "" && clientCertFile != "" {
		var err error

		// Import client certificate/key pair
		cert, err = tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, errors.Wrap(err, "unable to load ssl keypair")
		}

		// Just to print out the client certificate..
		cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse certificate")
		}

	}

	// Create tls.Config with desired tls properties
	return &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: tlsSkipVerify,
		Certificates:       []tls.Certificate{cert},
	}, nil
}

func (n *Natty) CreateConsumer(streamName, consumerName string) error {
	if _, err := n.js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: nats.AckExplicitPolicy,
	}); err != nil {
		// TODO: what happens if the consumer already exists? Does it error
		return errors.Wrap(err, "unable to create consumer")
	}

	return nil
}

func (n *Natty) DeleteConsumer(consumerName, streamName string) error {
	if err := n.js.DeleteConsumer(streamName, consumerName); err != nil {
		return errors.Wrap(err, "unable to delete consumer")
	}

	return nil
}

// Consume will create a durable consumer and consume messages from the configured stream
func (n *Natty) Consume(ctx context.Context, cfg *ConsumerConfig, f func(ctx context.Context, msg *nats.Msg) error) error {
	if err := validateConsumerConfig(cfg); err != nil {
		return errors.Wrap(err, "invalid consumer config")
	}

	if _, ok := ctx.Deadline(); !ok {
		return errors.New("context must have a deadline to work with NATS")
	}

	sub, err := n.js.PullSubscribe(cfg.Subject, cfg.ConsumerName)
	if err != nil {
		return errors.Wrap(err, "unable to create subscription")
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			n.log.Errorf("unable to unsubscribe from (stream: '%s', subj: '%s'): %s",
				cfg.StreamName, cfg.Subject, err)
		}
	}()

	var quit bool

	cfg.Looper.Loop(func() error {
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
					cfg.StreamName, cfg.Subject)

				cfg.Looper.Quit()
				quit = true

				return nil
			}

			if err == context.DeadlineExceeded {
				// No problem, context timed out - try again
				return nil
			}

			n.report(cfg.ErrorCh, fmt.Errorf("unable to fetch messages from (stream: '%s', subj: '%s'): %s",
				cfg.StreamName, cfg.Subject, err))

			return nil
		}

		for _, v := range msgs {
			if err := f(ctx, v); err != nil {
				n.report(cfg.ErrorCh, fmt.Errorf("callback func failed during message processing (stream: '%s', subj: '%s', msg: '%s'): %s",
					cfg.StreamName, cfg.Subject, v.Data, err))
			}
		}

		return nil
	})

	n.log.Debugf("consumer exiting (stream: %s, subj: %s)", cfg.StreamName, cfg.Subject)

	return nil
}

//func (n *Natty) Publish(ctx context.Context, subj string, msg []byte) error {
//	if _, err := n.js.Publish(subj, msg, nats.Context(ctx)); err != nil {
//		return errors.Wrap(err, "unable to publish message")
//	}
//
//	return nil
//}

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

func validateConsumerConfig(cfg *ConsumerConfig) error {
	if cfg.StreamName == "" {
		return ErrEmptyStreamName
	}

	if cfg.ConsumerName == "" {
		return ErrEmptyConsumerName
	}

	if cfg.Subject == "" {
		return ErrEmptySubject
	}

	// Apply optional defaults if needed
	if cfg.ErrorCh == nil {
		cfg.ErrorCh = make(chan error, 1)
	}

	if cfg.Looper == nil {
		cfg.Looper = director.NewFreeLooper(director.FOREVER, cfg.ErrorCh)
	}

	return nil
}

// ----------------------- publisher ------------------------

func (n *Natty) Publish(ctx context.Context, subject string, value []byte) {
	n.getPublisherBySubject(subject).batch(ctx, subject, value)
}

func (n *Natty) getPublisherBySubject(name string) *Publisher {
	n.publisherMutex.Lock()
	defer n.publisherMutex.Unlock()

	p, ok := n.publisherMap[name]
	if !ok {
		n.log.Debugf("creating new publisher goroutine for subject '%s'", name)

		p = n.newPublisher(uuid.NewV4().String())
		n.publisherMap[name] = p
	}

	return p
}

func (n *Natty) newPublisher(id string) *Publisher {
	return &Publisher{
		ID:         id,
		QueueMutex: &sync.RWMutex{},
		Queue:      make([]*message, 0),
		looper:     director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		nc:         n.nc,
		log:        n.log,
	}
}

func (p *Publisher) batch(_ context.Context, subject string, value []byte) {
	p.QueueMutex.Lock()
	defer p.QueueMutex.Unlock()

	p.Queue = append(p.Queue, &message{
		Subject: subject,
		Value:   value,
	})
}

// TODO: needed?
func buildBatch(slice []*message, entriesPerBatch int) [][]*message {
	batch := make([][]*message, 0)

	if len(slice) < entriesPerBatch {
		return append(batch, slice)
	}

	// How many iterations should we have?
	iterations := len(slice) / entriesPerBatch

	// We're operating in ints - we need the remainder
	remainder := len(slice) % entriesPerBatch

	var startIndex int
	nextIndex := entriesPerBatch

	for i := 0; i != iterations; i++ {
		batch = append(batch, slice[startIndex:nextIndex])

		startIndex = nextIndex
		nextIndex = nextIndex + entriesPerBatch
	}

	if remainder != 0 {
		batch = append(batch, slice[startIndex:])
	}

	return batch
}

func (p *Publisher) runBatchPublisher(ctx context.Context) {
	//var quit bool

	p.log.Debugf("publisher id '%s' exiting", p.ID)

	p.looper.Loop(func() error {
		p.QueueMutex.RLock()
		remaining := len(p.Queue)
		p.QueueMutex.RUnlock()

		if remaining == 0 {
			// empty queue, sleep for a bit and then loop again to check for new messages
			time.Sleep(time.Millisecond * 100)
			return nil
		}

		p.QueueMutex.Lock()
		batch := make([]*message, remaining)
		copy(p.Queue, batch)
		p.Queue = make([]*message, 0)
		p.QueueMutex.Unlock()

		if err := p.writeMessagesBatch(ctx, batch); err != nil {
			p.log.Error(err)
		}

		return nil
	})
}

func (p *Publisher) writeMessagesBatch(ctx context.Context, msgs []*message) error {
	p.log.Debugf("creating a batch for %d messages", len(msgs))

	// TODO: how to handle retry?
	// TODO: do we need batching? Can probably be eliminated since

	js, err := p.nc.JetStream(nats.PublishAsyncMaxPending(256)) // TODO: configure
	if err != nil {
		return errors.Wrap(err, "unable to create JetStream context")
	}

	for _, msg := range msgs {
		js.PublishAsync(msg.Subject, msg.Value)
	}

	select {
	case <-js.PublishAsyncComplete():
		p.log.Debugf("Successfully published '%d' messages", len(msgs))
		return nil
	case <-time.After(5 * time.Second): // TODO: configurable
		return errors.New("timed out waiting for message acknowledgement")
	}

	return nil
}
