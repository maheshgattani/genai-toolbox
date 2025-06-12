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

package pubsubcreatesubscription

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

func (m *mockSubscription) ID() string {
	args := m.Called()
	return args.String(0)
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

	// Set up expectations
	mockClient.On("Topic", "test-topic").Return(mockTopic)
	mockSub.On("ID").Return("test-sub")
	mockClient.On("CreateSubscription", mock.Anything, "test-sub", mock.Anything).Return(mockSub, nil)

	// Create tool with mock client
	tool := &Tool{
		source: &pubsub.Source{},
		params: tools.Parameters{
			tools.NewStringParameter("topicId", "The ID of the topic to subscribe to"),
			tools.NewStringParameter("subscriptionId", "The ID of the subscription to create"),
			tools.NewIntParameter("ackDeadlineSeconds", "The number of seconds after a subscriber receives a message before the subscriber should acknowledge the message"),
			tools.NewBoolParameter("enableMessageOrdering", "Whether to enable message ordering"),
			tools.NewStringParameter("filter", "An expression written in the Cloud Pub/Sub filter language"),
			tools.NewIntParameter("messageRetentionDuration", "How long to retain unacknowledged messages in the subscription's backlog"),
			tools.NewIntParameter("minimumBackoff", "The minimum delay between consecutive deliveries of a given message"),
			tools.NewIntParameter("maximumBackoff", "The maximum delay between consecutive deliveries of a given message"),
		},
	}

	// Test successful subscription creation
	params := tools.ParamValues{
		{Name: "topicId", Value: "test-topic"},
		{Name: "subscriptionId", Value: "test-sub"},
		{Name: "ackDeadlineSeconds", Value: 10},
		{Name: "enableMessageOrdering", Value: true},
		{Name: "filter", Value: "attributes.event_type = 'user_created'"},
		{Name: "messageRetentionDuration", Value: 3600},
		{Name: "minimumBackoff", Value: 1},
		{Name: "maximumBackoff", Value: 600},
	}

	result, err := tool.Invoke(context.Background(), params)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	subscription := result[0].(map[string]interface{})
	assert.Equal(t, "test-sub", subscription["subscriptionId"])
	assert.Equal(t, "test-topic", subscription["topicId"])

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
			tools.NewIntParameter("ackDeadlineSeconds", "The number of seconds after a subscriber receives a message before the subscriber should acknowledge the message"),
			tools.NewBoolParameter("enableMessageOrdering", "Whether to enable message ordering"),
			tools.NewStringParameter("filter", "An expression written in the Cloud Pub/Sub filter language"),
			tools.NewIntParameter("messageRetentionDuration", "How long to retain unacknowledged messages in the subscription's backlog"),
			tools.NewIntParameter("minimumBackoff", "The minimum delay between consecutive deliveries of a given message"),
			tools.NewIntParameter("maximumBackoff", "The maximum delay between consecutive deliveries of a given message"),
		},
	}

	manifest := tool.Manifest()
	assert.Equal(t, "Creates a new subscription to a Cloud Pub/Sub topic", manifest.Description)
	assert.Len(t, manifest.Parameters, 8)
	assert.Equal(t, "topicId", manifest.Parameters[0].Name)
	assert.Equal(t, "subscriptionId", manifest.Parameters[1].Name)
	assert.Equal(t, "ackDeadlineSeconds", manifest.Parameters[2].Name)
	assert.Equal(t, "enableMessageOrdering", manifest.Parameters[3].Name)
	assert.Equal(t, "filter", manifest.Parameters[4].Name)
	assert.Equal(t, "messageRetentionDuration", manifest.Parameters[5].Name)
	assert.Equal(t, "minimumBackoff", manifest.Parameters[6].Name)
	assert.Equal(t, "maximumBackoff", manifest.Parameters[7].Name)
}

func TestMcpManifest(t *testing.T) {
	tool := &Tool{
		params: tools.Parameters{
			tools.NewStringParameter("topicId", "The ID of the topic to subscribe to"),
			tools.NewStringParameter("subscriptionId", "The ID of the subscription to create"),
			tools.NewIntParameter("ackDeadlineSeconds", "The number of seconds after a subscriber receives a message before the subscriber should acknowledge the message"),
			tools.NewBoolParameter("enableMessageOrdering", "Whether to enable message ordering"),
			tools.NewStringParameter("filter", "An expression written in the Cloud Pub/Sub filter language"),
			tools.NewIntParameter("messageRetentionDuration", "How long to retain unacknowledged messages in the subscription's backlog"),
			tools.NewIntParameter("minimumBackoff", "The minimum delay between consecutive deliveries of a given message"),
			tools.NewIntParameter("maximumBackoff", "The maximum delay between consecutive deliveries of a given message"),
		},
	}

	manifest := tool.McpManifest()
	assert.Equal(t, ToolKind, manifest.Name)
	assert.Equal(t, "Creates a new subscription to a Cloud Pub/Sub topic", manifest.Description)
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