package consistent

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var c = New(20)

func init() {
	for i := 0; i < 3; i++ {
		c.Add(fmt.Sprintf("%d", i))
	}

	for i := 0; i < 3; i++ {
		c.Add(fmt.Sprintf("%d", i))
	}
}

func TestReplicas(t *testing.T) {

	mm := make(map[string]int)

	for i := 0; i < 3000; i++ {
		node, err := c.Get(fmt.Sprintf("queue_%d", i))

		require.NoError(t, err)

		mm[node] = mm[node] + 1
	}

	for k, v := range mm {
		println(k, v)
	}
}
