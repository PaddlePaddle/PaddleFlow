package job

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMultiply(t *testing.T) {
	cases := []struct {
		name     string
		replicas int
		res      string
		expected string
	}{
		{
			name:     "zero",
			replicas: 0,
			res:      "1T",
			expected: "0",
		},
		{
			name:     "zero",
			replicas: 100,
			res:      "0",
			expected: "0",
		},
		{
			name:     "normal multiple",
			replicas: 3,
			res:      "1M",
			expected: "3M",
		},
		{
			name:     "negative multiple",
			replicas: -3,
			res:      "1M",
			expected: "-3M",
		},
		{
			name:     "negative to positive multiple",
			replicas: -3,
			res:      "-3M",
			expected: "9M",
		},
		{
			name:     "unit change",
			replicas: 10000,
			res:      "3M",
			expected: "30G",
		},
	}

	for index, c := range cases {
		t.Logf("No.%d, case: %s", index+1, c.name)
		a := c.replicas
		actual, err := multiplyQuantity(a, c.res)
		assert.NoError(t, err)
		assert.Equal(t, c.expected, actual.String())
	}

}
