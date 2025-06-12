// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pubsubpublish

import (
	"context"
	"strings"
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	"github.com/googleapis/genai-toolbox/internal/sources/pubsub"
	"github.com/googleapis/genai-toolbox/internal/tools"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockPubSubClient struct {
	mock.Mock
}

func (m *mockPubSubClient) Topic(id string) *pubsub.Topic {
	args := m.Called(id)
	return args.Get(0).(*pubsub.Topic)
}

func (m *mockPubSubClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockTopic struct {
	mock.Mock
}

func (m *mockTopic) Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult {
	args := m.Called(ctx, msg)
	return args.Get(0).(*pubsub.PublishResult)
}

type mockPublishResult struct {
	mock.Mock
}

func (m *mockPublishResult) Get(ctx context.Context) (string, error) {
	args := m.Called(ctx)
	return args.String(0), args.Error(1)
}

func TestNewConfig(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		want    *Config
		wantErr bool
	}{
		{
			name: "valid config",
			yaml: `
source: my-pubsub
`,
			want: &Config{
				Source: "my-pubsub",
			},
			wantErr: false,
		},
		{
			name: "missing source",
			yaml: `
# no source specified
`,
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoder := yaml.NewDecoder(strings.NewReader(tt.yaml))
			got, err := NewConfig(context.Background(), "test", decoder)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if got.(*Config).Source != tt.want.Source {
					t.Errorf("NewConfig() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestToolConfigKind(t *testing.T) {
	config := &Config{}
	if got := config.ToolConfigKind(); got != ToolKind {
		t.Errorf("ToolConfigKind() = %v, want %v", got, ToolKind)
	}
}

func TestInitialize(t *testing.T) {
	config := &Config{
		Source: "test-pubsub",
	}

	// Create a mock Pub/Sub source
	mockSource := &pubsub.Source{}
	sources := map[string]sources.Source{
		"test-pubsub": mockSource,
	}

	tool, err := config.Initialize(sources)
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	// Verify tool type
	if _, ok := tool.(*Tool); !ok {
		t.Errorf("Initialize() returned wrong type: %T", tool)
	}
}

func TestInvoke(t *testing.T) {
	// Create mock objects
	mockClient := new(mockPubSubClient)
	mockTopic := new(mockTopic)
	mockResult := new(mockPublishResult)

	// Set up expectations
	mockClient.On("Topic", "test-topic").Return(mockTopic)
	mockTopic.On("Publish", mock.Anything, mock.Anything).Return(mockResult)
	mockResult.On("Get", mock.Anything).Return("test-message-id", nil)

	// Create tool with mock client
	tool := &Tool{
		source: &pubsub.Source{},
		params: tools.Parameters{
			tools.NewStringParameter("topicId", "The ID of the topic to publish to"),
			tools.NewStringParameter("message", "The message content to publish"),
			tools.NewMapParameter("attributes", "Optional attributes to attach to the message"),
		},
	}

	// Test successful publish
	params := tools.ParamValues{
		{Name: "topicId", Value: "test-topic"},
		{Name: "message", Value: "test message"},
		{Name: "attributes", Value: map[string]string{"key": "value"}},
	}

	result, err := tool.Invoke(context.Background(), params)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-message-id", result[0].(map[string]string)["messageId"])

	// Test missing required parameters
	_, err = tool.Invoke(context.Background(), tools.ParamValues{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "topicId parameter is required")

	// Test invalid message type
	_, err = tool.Invoke(context.Background(), tools.ParamValues{
		{Name: "topicId", Value: "test-topic"},
		{Name: "message", Value: 123}, // Invalid type
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "message parameter is required and must be a string")
}

func TestManifest(t *testing.T) {
	tool := &Tool{
		params: tools.Parameters{
			tools.NewStringParameter("topicId", "The ID of the topic to publish to"),
			tools.NewStringParameter("message", "The message content to publish"),
			tools.NewMapParameter("attributes", "Optional attributes to attach to the message"),
		},
	}

	manifest := tool.Manifest()
	assert.Equal(t, "Publishes a message to a Cloud Pub/Sub topic", manifest.Description)
	assert.Len(t, manifest.Parameters, 3)
	assert.Equal(t, "topicId", manifest.Parameters[0].Name)
	assert.Equal(t, "message", manifest.Parameters[1].Name)
	assert.Equal(t, "attributes", manifest.Parameters[2].Name)
}

func TestMcpManifest(t *testing.T) {
	tool := &Tool{
		params: tools.Parameters{
			tools.NewStringParameter("topicId", "The ID of the topic to publish to"),
			tools.NewStringParameter("message", "The message content to publish"),
			tools.NewMapParameter("attributes", "Optional attributes to attach to the message"),
		},
	}

	manifest := tool.McpManifest()
	assert.Equal(t, ToolKind, manifest.Name)
	assert.Equal(t, "Publishes a message to a Cloud Pub/Sub topic", manifest.Description)
	assert.NotNil(t, manifest.InputSchema)
}

func TestAuthorized(t *testing.T) {
	tool := &Tool{}

	tests := []struct {
		name                 string
		verifiedAuthServices []string
		want                 bool
	}{
		{
			name:                 "authorized",
			verifiedAuthServices: []string{"pubsub"},
			want:                 true,
		},
		{
			name:                 "not authorized",
			verifiedAuthServices: []string{"other"},
			want:                 false,
		},
		{
			name:                 "empty services",
			verifiedAuthServices: []string{},
			want:                 false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tool.Authorized(tt.verifiedAuthServices); got != tt.want {
				t.Errorf("Authorized() = %v, want %v", got, tt.want)
			}
		})
	}
} 