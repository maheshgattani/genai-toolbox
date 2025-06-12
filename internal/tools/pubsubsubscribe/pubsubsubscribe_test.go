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

package pubsubsubscribe

import (
	"context"
	"strings"
	"testing"
	"time"

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

func (m *mockPubSubClient) Subscription(id string) *pubsub.Subscription {
	args := m.Called(id)
	return args.Get(0).(*pubsub.Subscription)
}

func (m *mockPubSubClient) CreateSubscription(ctx context.Context, id string, config pubsub.SubscriptionConfig) (*pubsub.Subscription, error) {
	args := m.Called(ctx, id, config)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pubsub.Subscription), args.Error(1)
}

func (m *mockPubSubClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockTopic struct {
	mock.Mock
}

type mockSubscription struct {
	mock.Mock
}

func (m *mockSubscription) Exists(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
}

func (m *mockSubscription) Receive(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
	args := m.Called(ctx, f)
	return args.Error(0)
}

type mockMessage struct {
	mock.Mock
}

func (m *mockMessage) ID() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockMessage) Data() []byte {
	args := m.Called()
	return args.Get(0).([]byte)
}

func (m *mockMessage) Attributes() map[string]string {
	args := m.Called()
	return args.Get(0).(map[string]string)
}

func (m *mockMessage) Ack() {
	m.Called()
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
	mockSub := new(mockSubscription)
	mockMsg := new(mockMessage)

	// Set up expectations
	mockClient.On("Topic", "test-topic").Return(mockTopic)
	mockClient.On("Subscription", "test-sub").Return(mockSub)
	mockSub.On("Exists", mock.Anything).Return(false, nil)
	mockClient.On("CreateSubscription", mock.Anything, "test-sub", mock.Anything).Return(mockSub, nil)

	// Mock message
	mockMsg.On("ID").Return("test-message-id")
	mockMsg.On("Data").Return([]byte("test message"))
	mockMsg.On("Attributes").Return(map[string]string{"key": "value"})
	mockMsg.On("Ack").Return()

	// Mock subscription receive
	mockSub.On("Receive", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		f := args.Get(1).(func(context.Context, *pubsub.Message))
		ctx := context.Background()
		f(ctx, mockMsg)
	}).Return(nil)

	// Create tool with mock client
	tool := &Tool{
		source: &pubsub.Source{},
		params: tools.Parameters{
			tools.NewStringParameter("topicId", "The ID of the topic to subscribe to"),
			tools.NewStringParameter("subscriptionId", "The ID of the subscription to create"),
			tools.NewIntParameter("timeoutSeconds", "Number of seconds to wait for messages"),
			tools.NewIntParameter("maxMessages", "Maximum number of messages to receive"),
		},
	}

	// Test successful subscription
	params := tools.ParamValues{
		{Name: "topicId", Value: "test-topic"},
		{Name: "subscriptionId", Value: "test-sub"},
		{Name: "timeoutSeconds", Value: 5},
		{Name: "maxMessages", Value: 1},
	}

	result, err := tool.Invoke(context.Background(), params)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	messages := result[0].([]map[string]interface{})
	assert.Len(t, messages, 1)
	assert.Equal(t, "test-message-id", messages[0]["id"])
	assert.Equal(t, "test message", messages[0]["data"])
	assert.Equal(t, map[string]string{"key": "value"}, messages[0]["attributes"])

	// Test missing required parameters
	_, err = tool.Invoke(context.Background(), tools.ParamValues{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "topicId parameter is required")

	// Test invalid subscription ID
	_, err = tool.Invoke(context.Background(), tools.ParamValues{
		{Name: "topicId", Value: "test-topic"},
		{Name: "subscriptionId", Value: 123}, // Invalid type
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "subscriptionId parameter is required and must be a string")
}

func TestManifest(t *testing.T) {
	tool := &Tool{
		params: tools.Parameters{
			tools.NewStringParameter("topicId", "The ID of the topic to subscribe to"),
			tools.NewStringParameter("subscriptionId", "The ID of the subscription to create"),
			tools.NewIntParameter("timeoutSeconds", "Number of seconds to wait for messages"),
			tools.NewIntParameter("maxMessages", "Maximum number of messages to receive"),
		},
	}

	manifest := tool.Manifest()
	assert.Equal(t, "Subscribes to a Cloud Pub/Sub topic and receives messages", manifest.Description)
	assert.Len(t, manifest.Parameters, 4)
	assert.Equal(t, "topicId", manifest.Parameters[0].Name)
	assert.Equal(t, "subscriptionId", manifest.Parameters[1].Name)
	assert.Equal(t, "timeoutSeconds", manifest.Parameters[2].Name)
	assert.Equal(t, "maxMessages", manifest.Parameters[3].Name)
}

func TestMcpManifest(t *testing.T) {
	tool := &Tool{
		params: tools.Parameters{
			tools.NewStringParameter("topicId", "The ID of the topic to subscribe to"),
			tools.NewStringParameter("subscriptionId", "The ID of the subscription to create"),
			tools.NewIntParameter("timeoutSeconds", "Number of seconds to wait for messages"),
			tools.NewIntParameter("maxMessages", "Maximum number of messages to receive"),
		},
	}

	manifest := tool.McpManifest()
	assert.Equal(t, ToolKind, manifest.Name)
	assert.Equal(t, "Subscribes to a Cloud Pub/Sub topic and receives messages", manifest.Description)
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