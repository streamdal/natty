// NOTE: These tests require NATS to be available on "localhost"
package natty

import (
	"time"

	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"
)

var _ = Describe("KV", func() {
	var (
		cfg *Config
		n   *Natty
	)

	BeforeEach(func() {
		var err error

		cfg = NewConfig()

		n, err = New(cfg)

		Expect(err).To(BeNil())
		Expect(n).NotTo(BeNil())
	})

	Describe("Get", func() {
		It("should return the value for a key", func() {
			bucket, key, value := NewKVSet()

			kv, err := n.js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:      bucket,
				Description: "created during kv test",
			})

			Expect(err).To(BeNil())

			Expect(kv).NotTo(BeNil())

			_, putErr := kv.Put(key, []byte(value))
			Expect(putErr).To(BeNil())

			data, err := n.Get(nil, bucket, key)
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(Equal([]byte(value)))
		})

		It("should not auto-create a bucket", func() {
			data, err := n.Get(nil, "non-existent-bucket", "non-existent-key")
			Expect(err).To(Equal(nats.ErrKeyNotFound))
			Expect(data).To(BeNil())

			kv, err := n.js.KeyValue("non-existent-bucket")
			Expect(err).To(Equal(nats.ErrBucketNotFound))
			Expect(kv).To(BeNil())
		})
	})

	Describe("Put", func() {
		It("should set the value for a key (and auto-create the bucket)", func() {
			bucket, key, value := NewKVSet()

			putErr := n.Put(nil, bucket, key, value)
			Expect(putErr).ToNot(HaveOccurred())

			// Bucket should've been created
			kv, err := n.js.KeyValue(bucket)
			Expect(err).ToNot(HaveOccurred())
			Expect(kv).NotTo(BeNil())

			// K/V should've been created
			kve, err := kv.Get(key)
			Expect(err).ToNot(HaveOccurred())
			Expect(kve).NotTo(BeNil())

			// Values should match
			Expect(kve.Value()).To(Equal(value))
		})

		It("a key with a TTL will get auto expired", func() {
			bucket, key, value := NewKVSet()

			putErr := n.Put(nil, bucket, key, value, 1*time.Second)
			Expect(putErr).ToNot(HaveOccurred())

			// Bucket should've been created
			kv, err := n.js.KeyValue(bucket)
			Expect(err).ToNot(HaveOccurred())
			Expect(kv).NotTo(BeNil())

			// Wait a couple sec
			time.Sleep(2 * time.Second)

			// K/V should no longer be there
			kve, err := kv.Get(key)
			Expect(err).To(HaveOccurred())
			Expect(kve).To(BeNil())
			Expect(err).To(Equal(nats.ErrKeyNotFound))

		})
	})

	Describe("Delete", func() {
		It("should delete the value for a key", func() {
			bucket, key, value := NewKVSet()

			// Create a bucket + key
			kv, err := n.js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:      bucket,
				Description: "created during kv test",
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(kv).ToNot(BeNil())

			_, putErr := kv.Put(key, value)
			Expect(putErr).ToNot(HaveOccurred())

			// Try to delete it
			delErr := n.Delete(nil, bucket, key)
			Expect(delErr).ToNot(HaveOccurred())

			// Check via js context that it's gone
			_, getErr := kv.Get(key)
			Expect(getErr).To(Equal(nats.ErrKeyDeleted))
		})
	})
})

func NewKVSet() (bucket string, key string, value []byte) {
	bucket = uuid.NewV4().String()
	key = uuid.NewV4().String()
	value = []byte(uuid.NewV4().String())

	testBuckets = append(testBuckets, bucket)

	return
}
