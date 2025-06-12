package pubsubdeletetopic

import (
	"context"
	"testing"

	"github.com/googleapis/genai-toolbox/internal/sources/pubsub"
	"github.com/googleapis/genai-toolbox/internal/tools"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewConfig(t *testing.T) {
	config := NewConfig()
	assert.Equal(t, ToolKind, config.(*Config).Kind)
	assert.Equal(t, "Delete a Pub/Sub topic", config.(*Config).Description)
}

func TestInitialize(t *testing.T) {
	config := &Config{
		Name:         "delete-topic",
		Kind:         ToolKind,
		Source:       "pubsub",
		Description:  "Delete a Pub/Sub topic",
		AuthRequired: "my-auth-service",
	}

	mockSource := &pubsub.MockSource{}
	sources := map[string]tools.Source{
		"pubsub": mockSource,
	}

	tool, err := config.Initialize(sources)
	assert.NoError(t, err)
	assert.NotNil(t, tool)
	assert.Equal(t, "delete-topic", tool.Name())
	assert.Equal(t, ToolKind, tool.Kind())
	assert.Equal(t, "Delete a Pub/Sub topic", tool.Description())
}

func TestInvoke(t *testing.T) {
	config := &Config{
		Name:         "delete-topic",
		Kind:         ToolKind,
		Source:       "pubsub",
		Description:  "Delete a Pub/Sub topic",
		AuthRequired: "my-auth-service",
	}

	mockSource := &pubsub.MockSource{}
	sources := map[string]tools.Source{
		"pubsub": mockSource,
	}

	tool, err := config.Initialize(sources)
	assert.NoError(t, err)

	// Test successful deletion
	mockSource.On("DeleteTopic", mock.Anything, "test-topic").Return(nil)
	result, err := tool.Invoke(context.Background(), map[string]interface{}{
		"topic": "test-topic",
	})
	assert.NoError(t, err)
	assert.Equal(t, "Successfully deleted topic: test-topic", result.(map[string]interface{})["message"])

	// Test missing topic parameter
	_, err = tool.Invoke(context.Background(), map[string]interface{}{})
	assert.Error(t, err)
	assert.Equal(t, "topic parameter is required", err.Error())

	// Test deletion error
	mockSource.On("DeleteTopic", mock.Anything, "error-topic").Return(assert.AnError)
	_, err = tool.Invoke(context.Background(), map[string]interface{}{
		"topic": "error-topic",
	})
	assert.Error(t, err)
	assert.Equal(t, "failed to delete topic: assert.AnError general error for testing", err.Error())
}
