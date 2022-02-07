package natty

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestNattySuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Natty Suite")
}
