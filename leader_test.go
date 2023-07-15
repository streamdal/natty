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
			cfg := NewAsLeaderConfig("batman", MustNewUUID(), MustNewUUID())

			err := n.AsLeader(context.Background(), cfg, nil)
			Expect(err.Error()).To(ContainSubstring("Function cannot be nil"))

			// Quick, non-extensive check
			cfg.Looper = nil

			err = n.AsLeader(context.Background(), cfg, func() error { return nil })
			Expect(err.Error()).To(ContainSubstring("Looper is required"))
		})

		It("should exec func when leader", func() {
			executedBy := make([]string, 0)

			errChan := make(chan error, 10)
			cancelCtx, cancel := context.WithCancel(context.Background())
			bucketName := MustNewUUID()
			keyName := MustNewUUID()

			for i := 0; i < 3; i++ {
				cfg := NewAsLeaderConfig(fmt.Sprintf("node-%d", i), bucketName, keyName)

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

		It("should become leader when initial leader fails", func() {
			// 1. Launch two AsLeader's - one should become leader
			// 2. Shutdown AsLeader 1 - AsLeader 2 should become leader
			// 3. Shutdown AsLeader 2, start AsLeader 1 - AsLeader 1 should become leader

			executedBy := make(map[string]int)

			errChan := make(chan error, 10)
			cancelCtx1, cancel1 := context.WithCancel(context.Background())
			cancelCtx2, cancel2 := context.WithCancel(context.Background())
			bucketName := MustNewUUID()
			keyName := MustNewUUID()

			cfg1 := NewAsLeaderConfig(fmt.Sprintf("node-1"), bucketName, keyName)

			// Start node 1
			startupLeader(cancelCtx1, n, &cfg1, errChan, func() error {
				executedBy[cfg1.NodeName] += 1
				return nil
			})

			// Sleep so that node1 is guaranteed to become leader
			time.Sleep(time.Second)

			cfg2 := NewAsLeaderConfig(fmt.Sprintf("node-2"), bucketName, keyName)

			// Start node 2
			startupLeader(cancelCtx2, n, &cfg2, errChan, func() error {
				executedBy[cfg2.NodeName] += 1
				return nil
			})

			// Expect for AsLeader to not error for 2s
			Consistently(errChan, "2s").ShouldNot(Receive())

			// Expect only one leader to have ever executed
			Expect(len(executedBy)).To(Equal(1), "only one leader should have executed the func")
			Expect(executedBy["node-1"]).ToNot(BeNil())

			// Shutdown AsLeader 1
			cancel1()

			// Clear the map to have a clean state
			executedBy = make(map[string]int)

			// Give a moment for as leader 1 to exit
			time.Sleep(2 * time.Second)

			// Expect leader 2 to have become leader and executed func
			Expect(len(executedBy)).To(Equal(1), "only one leader should have executed the func")
			Expect(executedBy["node-2"]).ToNot(BeNil())

			// Shutdown AsLeader 2, startup AsLeader 1
			cancel2()

			// Give AsLeader 2 to exit
			time.Sleep(2 * time.Second)

			// Reset executedBy again
			executedBy = make(map[string]int)

			cancelCtx1, cancel1 = context.WithCancel(context.Background())

			startupLeader(cancelCtx1, n, &cfg1, errChan, func() error {
				executedBy[cfg1.NodeName] += 1
				return nil
			})

			// Give AsLeader 1 to exec a few times
			time.Sleep(2 * time.Second)

			// Assert AsLeader 1 had become leader again
			Expect(len(executedBy)).To(Equal(1), "only one leader should have executed the func")
			Expect(executedBy["node-1"]).ToNot(BeNil())

			cancel1()
		})

		It("should not exec function when not leader", func() {
			// Get leader
			iran := false

			errChan := make(chan error, 10)
			cancelCtx, cancel := context.WithCancel(context.Background())
			bucketName := MustNewUUID()
			keyName := MustNewUUID()

			cfg := NewAsLeaderConfig("node-1", bucketName, keyName)

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

			cfg2 := NewAsLeaderConfig("node-2", bucketName, keyName)
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
			bucketName := MustNewUUID()
			keyName := MustNewUUID()

			kv, err := n.js.KeyValue(bucketName)
			Expect(err).To(Equal(nats.ErrBucketNotFound))
			Expect(kv).To(BeNil())

			// Start AsLeader
			errChan := make(chan error, 10)
			cancelCtx, cancel := context.WithCancel(context.Background())

			cfg := NewAsLeaderConfig("node-1", bucketName, keyName)

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
			bucketName := MustNewUUID()
			keyName := MustNewUUID()
			bucketTTL := time.Second

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

			cfg := NewAsLeaderConfig("node-1", bucketName, keyName)
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
			bucketName := MustNewUUID()
			keyName := MustNewUUID()
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

			cfg := NewAsLeaderConfig("node-1", bucketName, keyName)

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

		It("HaveLeader should work", func() {
			// Launch 3 AsLeaders, figure out who's the leader, run HaveLeader
			// with each cfg - two should not have leader, one should have leader
			executedBy := make([]string, 0)

			errChan := make(chan error, 10)
			cancelCtx, cancel := context.WithCancel(context.Background())
			bucketName := MustNewUUID()
			keyName := MustNewUUID()
			cfgs := make(map[string]AsLeaderConfig)

			for i := 0; i < 3; i++ {
				nodeName := fmt.Sprintf("node-%d", i)

				cfg := NewAsLeaderConfig(nodeName, bucketName, keyName)

				cfgs[nodeName] = cfg

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

			var leader string

			// Figure out leader
			for _, runner := range executedBy {
				if leader == "" {
					leader = runner
					continue
				}

				// All other execs should be done by the same leader
				Expect(runner).To(Equal(leader))
			}

			// Verify HaveLeader against each cfg
			for _, cfg := range cfgs {
				if cfg.NodeName == leader {
					Expect(n.HaveLeader(context.Background(), cfg.NodeName, cfg.Bucket, cfg.Key)).To(BeTrue())
				} else {
					Expect(n.HaveLeader(context.Background(), cfg.NodeName, cfg.Bucket, cfg.Key)).To(BeFalse())
				}
			}
		})

		It("validateAsLeaderConfig", func() {
			type AsLeaderConfigEntry struct {
				Config        *AsLeaderConfig
				Description   string
				ShouldError   bool
				ErrorContains string
			}

			tests := []AsLeaderConfigEntry{
				{
					Config:        nil,
					Description:   "nil config should fail",
					ShouldError:   true,
					ErrorContains: "AsLeaderConfig is required",
				},
				{
					Config:        &AsLeaderConfig{},
					Description:   "non filled out config",
					ShouldError:   true,
					ErrorContains: "Looper is required",
				},
				{
					Config: &AsLeaderConfig{
						Looper:   director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
						Bucket:   "",
						Key:      "test",
						NodeName: "test",
					},
					Description:   "empty bucket name should fail",
					ShouldError:   true,
					ErrorContains: "Bucket is required",
				},
				{
					Config: &AsLeaderConfig{
						Looper:   director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
						Bucket:   "test",
						Key:      "",
						NodeName: "test",
					},
					Description:   "empty key name should fail",
					ShouldError:   true,
					ErrorContains: "Key is required",
				},
				{
					Config: &AsLeaderConfig{
						Looper:   director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
						Bucket:   "test",
						Key:      "test",
						NodeName: "",
					},
					Description:   "empty node name should fail",
					ShouldError:   true,
					ErrorContains: "NodeName is required",
				},
				{
					Config: &AsLeaderConfig{
						Looper:   nil,
						Bucket:   "test",
						Key:      "test",
						NodeName: "test",
					},
					Description:   "should error with nil looper",
					ShouldError:   true,
					ErrorContains: "Looper is required",
				},
				{
					Config: &AsLeaderConfig{
						Looper:   director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
						Bucket:   "test",
						Key:      "test",
						NodeName: "test",
					},
					Description: "should not error with all required fields filled out",
					ShouldError: false,
				},
			}

			for _, test := range tests {
				if test.ShouldError {
					err := validateAsLeaderArgs(test.Config, func() error { return nil })

					Expect(err).To(HaveOccurred(), test.Description)
					Expect(err.Error()).To(ContainSubstring(test.ErrorContains), test.Description)
				} else {
					err := validateAsLeaderArgs(test.Config, func() error { return nil })

					Expect(err).ToNot(HaveOccurred(), test.Description)
				}
			}
		})
	})
})

func NewAsLeaderConfig(nodeName, bucketName, keyName string) AsLeaderConfig {
	cfg := AsLeaderConfig{
		Bucket:      bucketName,
		Key:         keyName,
		Description: "AsLeader test",
		NodeName:    nodeName,

		// Using a fast looper for quicker tests
		Looper: director.NewTimedLooper(director.FOREVER, 200*time.Millisecond, make(chan error, 1)),

		// Overriding default looper for quicker tests
		ElectionLooper: director.NewTimedLooper(director.FOREVER, 25*time.Millisecond, make(chan error, 1)),

		// Overriding bucket TTL for quicker tests
		BucketTTL: 250 * time.Millisecond,
	}

	usedBuckets = append(usedBuckets, cfg.Bucket)

	return cfg
}

func startupLeader(ctx context.Context, n INatty, cfg *AsLeaderConfig, errChan chan error, f func() error) {
	go func() {
		defer GinkgoRecover()

		err := n.AsLeader(ctx, *cfg, f)

		if err != nil {
			errChan <- err
		}
	}()
}
