package transformer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJoin_MultipleItems(t *testing.T) {
	assert.Equal(t, "a, b, c", Join([]string{"a", "b", "c"}))
}

func TestJoin_SingleItem(t *testing.T) {
	assert.Equal(t, "x", Join([]string{"x"}))
}

func TestJoin_EmptySlice(t *testing.T) {
	assert.Equal(t, "", Join([]string{}))
}

func TestJoin_NilSlice(t *testing.T) {
	assert.Equal(t, "", Join(nil))
}
