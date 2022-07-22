// NOTE: These tests require NATS to be available on "localhost"
package natty

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/relistan/go-director"
	uuid "github.com/satori/go.uuid"
)

var (
	usedBuckets = make([]string, 0)
)

var _ = Describe("KV", func() {
	var (
		cfg *Config
		n   *Natty
	)

	BeforeEach(func() {
		var beforeEachErr error

		cfg = NewConfig()

		// Enable for test debug
		//logger := logrus.New()
		//logger.Level = logrus.DebugLevel
		//cfg.Logger = logger

		n, beforeEachErr = New(cfg)

		Expect(beforeEachErr).To(BeNil())
		Expect(n).NotTo(BeNil())
	})

	AfterEach(func() {
		err := CleanupBuckets(usedBuckets)
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("AsLeader", func() {
		It("should error with bad config", func() {
			cfg := NewAsLeaderConfig("batman", uuid.NewV4().String())

			err := n.AsLeader(context.Background(), nil, nil)
			Expect(err.Error()).To(ContainSubstring("AsLeaderConfig is required"))

			err = n.AsLeader(context.Background(), cfg, nil)
			Expect(err.Error()).To(ContainSubstring("func is required"))

		})

		It("should exec func when leader", func() {
			executedBy := make([]string, 0)

			errChan := make(chan error, 10)
			cancelCtx, cancel := context.WithCancel(context.Background())
			bucketName := uuid.NewV4().String()

			for i := 0; i < 3; i++ {
				cfg := NewAsLeaderConfig(fmt.Sprintf("node-%d", i), bucketName)

				go func() {
					defer GinkgoRecover()

					err := n.AsLeader(cancelCtx, cfg, func() error {
						executedBy = append(executedBy, cfg.NodeName)
						return nil
					})

					if err != nil {
						errChan <- err
					}
				}()
			}

			// Expect for AsLeader to not error for 2s
			Consistently(errChan, "2s").ShouldNot(Receive())

			// Shutdown goroutines
			cancel()

			// Timing exactly how many times exec func would be called is difficult
			// to predict as it can be affected by CPU load (and probably other factors).
			// Best we can do is expect that it has happened at least a couple of times.
			Expect(len(executedBy)).To(BeNumerically(">=", 2))

			// Ensure that same leader has always executed func
			var expectedNode string

			for _, v := range executedBy {
				if expectedNode == "" {
					expectedNode = v
					continue
				}

				Expect(expectedNode).To(Equal(v))
			}

			// For good measure, verify that expectedNode is actually set
			Expect(expectedNode).ToNot(BeEmpty())
		})

		It("should not exec function when not leader", func() {
			// Get leader
			iran := false

			errChan := make(chan error, 10)
			cancelCtx, cancel := context.WithCancel(context.Background())
			bucketName := uuid.NewV4().String()

			cfg := NewAsLeaderConfig("node-1", bucketName)

			go func() {
				defer GinkgoRecover()

				err := n.AsLeader(cancelCtx, cfg, func() error {
					iran = true
					return nil
				})

				if err != nil {
					errChan <- err
				}
			}()

			// Leader should not error for 2s
			Consistently(errChan, "2s").ShouldNot(Receive())

			// Expect that leader has ran func
			Expect(iran).To(BeTrue())

			/////////////// Node #2 //////////////////////

			// Start another AsLeader
			var iran2 bool

			cfg2 := NewAsLeaderConfig("node-2", bucketName)
			errChan2 := make(chan error, 10)

			go func() {
				defer GinkgoRecover()

				err := n.AsLeader(cancelCtx, cfg2, func() error {
					iran2 = true
					return nil
				})

				if err != nil {
					errChan2 <- err
				}
			}()

			// Verify that 2nd node has not emitted errors
			Consistently(errChan2, "2s").ShouldNot(Receive())

			// Verify that 2nd AsLeader hasn't executed func
			Expect(iran2).To(BeFalse())

			// Stop all goroutines
			cancel()
		})

		It("should auto-create bucket", func() {
			// Verify bucket doesn't exist
			bucketName := uuid.NewV4().String()

			kv, err := n.js.KeyValue(bucketName)
			Expect(err).To(Equal(nats.ErrBucketNotFound))
			Expect(kv).To(BeNil())

			// Start AsLeader
			errChan := make(chan error, 10)
			cancelCtx, cancel := context.WithCancel(context.Background())

			cfg := NewAsLeaderConfig("node-1", bucketName)

			go func() {
				defer GinkgoRecover()

				err := n.AsLeader(cancelCtx, cfg, func() error {
					return nil
				})

				if err != nil {
					errChan <- err
				}
			}()

			// Leader should not error for 2s
			Consistently(errChan, "2s").ShouldNot(Receive())

			cancel()

			// Verify bucket exists
			kv, err = n.js.KeyValue(bucketName)
			Expect(err).ToNot(HaveOccurred())
			Expect(kv).ToNot(BeNil())
		})

		It("should work with existing bucket and same TTL", func() {
			bucketName := uuid.NewV4().String()
			bucketTTL := 321 * time.Second

			// Create bucket manually with TTL
			kv, err := n.js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:      bucketName,
				Description: "AsLeader bucket auto-create test",
				TTL:         bucketTTL,
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(kv).ToNot(BeNil())

			// Start AsLeader with same TTL <- shouldn't error
			errChan := make(chan error, 10)
			cancelCtx, cancel := context.WithCancel(context.Background())

			cfg := NewAsLeaderConfig("node-1", bucketName)
			cfg.BucketTTL = bucketTTL

			go func() {
				defer GinkgoRecover()

				err := n.AsLeader(cancelCtx, cfg, func() error {
					return nil
				})

				if err != nil {
					n.log.Debugf("AsLeader error: %v", err)
					errChan <- err
				}
			}()

			// Leader should not error for 2s
			Consistently(errChan, "2s").ShouldNot(Receive())

			cancel()
		})

		It("should error when pre-existing bucket has different TTL", func() {
			bucketName := uuid.NewV4().String()
			bucketTTL := 321 * time.Second

			// Create bucket manually with TTL
			kv, err := n.js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:      bucketName,
				Description: "AsLeader bucket auto-create test",
				TTL:         bucketTTL,
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(kv).ToNot(BeNil())

			// Start AsLeader with same TTL <- shouldn't error
			var errorHasOccurred error
			cancelCtx, cancel := context.WithCancel(context.Background())
			errChan := make(chan error, 10)

			cfg := NewAsLeaderConfig("node-1", bucketName)
			cfg.BucketTTL = time.Second * 123456789

			go func() {
				errorHasOccurred = n.AsLeader(cancelCtx, cfg, func() error {
					return nil
				})

				if errorHasOccurred != nil {
					errChan <- errorHasOccurred
				}
			}()

			// We should get an error
			Eventually(errChan, "2s").Should(Receive(MatchError(ErrBucketTTLMismatch)))

			cancel()

		})
	})
})

func NewAsLeaderConfig(nodeName string, bucketName string) *AsLeaderConfig {
	cfg := &AsLeaderConfig{
		Bucket:         bucketName,
		Description:    "AsLeader test",
		BucketTTL:      time.Second * 10,
		NodeName:       nodeName,
		Looper:         director.NewTimedLooper(director.FOREVER, 200*time.Millisecond, make(chan error, 1)),
		ElectionLooper: director.NewTimedLooper(director.FOREVER, 100*time.Millisecond, make(chan error, 1)),
	}

	usedBuckets = append(usedBuckets, cfg.Bucket)

	return cfg
}
