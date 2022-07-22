package natty

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/relistan/go-director"
)

const (
	DefaultBucketTTl       = time.Second * 10
	ElectionLooperInterval = time.Second * 3
)

var (
	ErrBucketTTLMismatch = errors.New("bucket ttl mismatch")
)

type AsLeaderConfig struct {
	// Looper is the loop construct that will be used to execute Func (required)
	Looper director.Looper

	// Bucket specifies what K/V bucket will be used for leader election (required)
	Bucket string

	// NodeName is the name used for this node (should be unique in cluster) (optional)
	NodeName string

	// BucketTTL specifies the TTL of values in the bucket (optional; default = 10s)
	BucketTTL time.Duration

	// ElectionLooper specifies the loop construct that will be used to perform leader election attempts (optional)
	ElectionLooper director.Looper

	// Description will set the bucket description (optional)
	Description string

	// Internal fields
	haveLeader bool
}

func (n *Natty) AsLeader(ctx context.Context, cfg *AsLeaderConfig, f func() error) error {
	if err := validateAsLeaderConfig(cfg); err != nil {
		return errors.Wrap(err, "unable to validate AsLeaderConfig")
	}

	if f == nil {
		return errors.New("func is required")
	}

	// Attempt to create bucket; if bucket exists, verify that TTL matches
	if err := n.CreateBucket(ctx, cfg.Bucket, cfg.BucketTTL, cfg.Description); err != nil {
		if strings.Contains(err.Error(), "stream name already in use") {
			n.log.Debug("bucket exists, checking if ttl matches")

			kv, err := n.js.KeyValue(cfg.Bucket)
			if err != nil {
				return errors.Wrap(err, "unable to fetch existing bucket")
			}

			s, err := kv.Status()
			if err != nil {
				return errors.Wrap(err, "unable to fetch existing bucket status")
			}

			n.log.Debugf("ttl on bucket: %v desired ttl: %v", s.TTL(), cfg.BucketTTL)

			if s.TTL().Seconds() != cfg.BucketTTL.Seconds() {
				n.log.Error("bucket ttls do not match")
				return ErrBucketTTLMismatch
			}
		} else {
			return errors.Wrap(err, "unable to pre-create leader bucket")
		}
	}

	errCh := make(chan error, 1)

	n.log.Debugf("%s: starting leader election goroutine", cfg.NodeName)

	// Launch leader election in goroutine (leader election goroutine should quit when AsLeader is cancelled or exits)
	go func() {
		err := n.runLeaderElection(ctx, cfg)
		if err != nil {
			n.log.Errorf("%s: unable to run leader election: %v", cfg.NodeName, err)
			errCh <- err

			return
		}
	}()

	n.log.Debugf("%s: waiting for goroutine to not error", cfg.NodeName)

	select {
	case err := <-errCh:
		return errors.Wrap(err, "ran into error during leader election startup")
	case <-ctx.Done():
		return errors.New("context cancelled - leader election cancelled")
		// Leader election did not error after 2 seconds, all is well
	case <-time.After(time.Second * 1):
		break
	}

	// Leader election goroutine started; run main loop
	n.log.Debugf("%s: leader election goroutine started; running main loop", cfg.NodeName)

	cfg.Looper.Loop(func() error {
		if !cfg.haveLeader {
			n.log.Debugf("%s: AsLeader: not leader", cfg.NodeName)
			return nil
		}

		n.log.Debugf("%s: AsLeader: running func", cfg.NodeName)

		// Have leader, exec func
		if err := f(); err != nil {
			n.log.Errorf("%s: error during func execution: %v", cfg.NodeName, err)
			return nil
		}

		return nil
	})

	return nil
}

func (n *Natty) runLeaderElection(ctx context.Context, cfg *AsLeaderConfig) error {
	var quit bool

	cfg.ElectionLooper.Loop(func() error {
		// We are supposed to quit - give looper time to react to quit
		if quit {
			time.Sleep(time.Second)
			return nil
		}

		// NATS K/V client does not support ctx yet so we do it here instead
		select {
		case <-ctx.Done():
			n.log.Debugf("%s: context cancelled, exiting leader election", cfg.NodeName)
			quit = true
			cfg.ElectionLooper.Quit()

			return nil
		default:
			// Continue
		}

		// Have leader - attempt to update key to increase TTL
		if cfg.haveLeader {
			if err := n.Put(ctx, cfg.Bucket, "leader", []byte(cfg.NodeName)); err != nil {
				n.log.Errorf("%s: unable to update leader key: %v", cfg.NodeName, err)
				cfg.haveLeader = false

				return nil
			}

			n.log.Debugf("%s: updated leader key", cfg.NodeName)

			return nil
		}

		if err := n.Create(ctx, cfg.Bucket, "leader", []byte(cfg.NodeName)); err != nil {
			if strings.Contains(err.Error(), "wrong last sequence") {
				n.log.Debugf("%s: leader key already exists, ignoring", cfg.NodeName)
				return nil
			}

			n.log.Errorf("%s: unable to create leader key: %v", cfg.NodeName, err)

			return nil
		}

		n.log.Debugf("%s: leader key created", cfg.NodeName)

		// Have leader
		cfg.haveLeader = true

		return nil
	})

	n.log.Debugf("%s: leader election goroutine exiting", cfg.NodeName)

	return nil
}

func validateAsLeaderConfig(cfg *AsLeaderConfig) error {
	if cfg == nil {
		return errors.New("AsLeaderConfig is required")
	}

	if cfg.Looper == nil {
		return errors.New("Looper is required")
	}

	if cfg.Bucket == "" {
		return errors.New("Bucket is required")
	}

	if cfg.BucketTTL == 0 {
		cfg.BucketTTL = DefaultBucketTTl
	}

	if cfg.ElectionLooper == nil {
		cfg.ElectionLooper = director.NewTimedLooper(director.FOREVER, ElectionLooperInterval, make(chan error, 1))
	}

	return nil
}
