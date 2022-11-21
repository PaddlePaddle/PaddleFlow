package resources

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuantity_AsInt64(t *testing.T) {
	res := "100k"
	q, err := ParseQuantity(res)
	assert.NoError(t, err)
	t.Logf("q.AsInt64()=%d", q.AsInt64())
}
