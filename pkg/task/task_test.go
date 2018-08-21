package task

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	newPool := New(4)
	assert.NotNil(t, newPool)

	invalidPoolParam := New(-1)
	assert.Nil(t, invalidPoolParam)

}

func TestStartStop(t *testing.T) {
	var myPool = New(2)
	assert.NotNil(t, myPool)

	myPool.Start()
	assert.True(t, myPool.isStarted())

	myPool.Stop()
	assert.False(t, myPool.isStarted())
}
