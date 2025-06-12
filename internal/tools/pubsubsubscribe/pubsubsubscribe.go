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
	"fmt"
	"time"

	cloudpubsub "cloud.google.com/go/pubsub"
	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	"github.com/googleapis/genai-toolbox/internal/sources/pubsub"
	"github.com/googleapis/genai-toolbox/internal/tools"
)

const ToolKind = "pubsubsubscribe"

// Config represents the configuration for the Pub/Sub subscribe tool.
type Config struct {
	Name         string   `yaml:"name" validate:"required"`
	Kind         string   `yaml:"kind" validate:"required"`
	Source       string   `yaml:"source"`
	Description  string   `yaml:"description" validate:"required"`
	AuthRequired []string `yaml:"authRequired"`
}

// Tool implements the tools.Tool interface for subscribing to Pub/Sub topics.
type Tool struct {
	source       *pubsub.Source
	params       tools.Parameters
	Name         string
	mcpManifest  tools.McpManifest
	authRequired []string
}

func init() {
	tools.Register(ToolKind, NewConfig)
}

// NewConfig creates a new configuration for the Pub/Sub subscribe tool.
func NewConfig(ctx context.Context, name string, decoder *yaml.Decoder) (tools.ToolConfig, error) {
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode config: %w", err)
	}

	if config.Source == "" {
		return nil, fmt.Errorf("source is required")
	}

	config.Name = name
	config.Kind = ToolKind
	config.Description = "Subscribes to a Cloud Pub/Sub topic and receives messages"

	return &config, nil
}

// ToolConfigKind returns the kind of the tool configuration.
func (c *Config) ToolConfigKind() string {
	return ToolKind
}

// Initialize initializes the tool with the given sources.
func (c *Config) Initialize(sources map[string]sources.Source) (tools.Tool, error) {
	source, ok := sources[c.Source].(*pubsub.Source)
	if !ok {
		return nil, fmt.Errorf("source %q is not a Pub/Sub source", c.Source)
	}

	params := tools.Parameters{
		tools.NewStringParameter("topicId", "The ID of the topic to subscribe to"),
		tools.NewStringParameter("subscriptionId", "The ID of the subscription to use"),
		tools.NewIntParameter("timeoutSeconds", "The number of seconds to wait for messages"),
		tools.NewIntParameter("maxMessages", "The maximum number of messages to receive"),
	}

	mcpManifest := tools.McpManifest{
		Name:        c.Name,
		Description: c.Description,
		InputSchema: params.McpManifest(),
	}

	return &Tool{
		source:       source,
		params:       params,
		Name:         c.Name,
		mcpManifest:  mcpManifest,
		authRequired: c.AuthRequired,
	}, nil
}

// Invoke subscribes to a Pub/Sub topic and receives messages.
func (t *Tool) Invoke(ctx context.Context, params tools.ParamValues) ([]any, error) {
	paramsMap := params.AsMap()
	topicId, ok := paramsMap["topicId"].(string)
	if !ok {
		return nil, fmt.Errorf("topicId parameter is required and must be a string")
	}

	subscriptionId, ok := paramsMap["subscriptionId"].(string)
	if !ok {
		return nil, fmt.Errorf("subscriptionId parameter is required and must be a string")
	}

	timeoutSeconds, ok := paramsMap["timeoutSeconds"].(int)
	if !ok {
		timeoutSeconds = 30 // Default timeout
	}

	maxMessages, ok := paramsMap["maxMessages"].(int)
	if !ok {
		maxMessages = 10 // Default max messages
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	// Get the subscription
	sub := t.source.Client().Subscription(subscriptionId)

	// Create a channel to receive messages
	msgChan := make(chan *cloudpubsub.Message, maxMessages)
	errChan := make(chan error, 1)

	// Start receiving messages
	go func() {
		err := sub.Receive(ctx, func(ctx context.Context, msg *cloudpubsub.Message) {
			msgChan <- msg
			msg.Ack()
		})
		if err != nil {
			errChan <- err
		}
	}()

	// Collect messages until we reach maxMessages or timeout
	var messages []map[string]any
	for i := 0; i < maxMessages; i++ {
		select {
		case msg := <-msgChan:
			messages = append(messages, map[string]any{
				"id":         msg.ID,
				"data":       string(msg.Data),
				"attributes": msg.Attributes,
			})
		case err := <-errChan:
			return nil, fmt.Errorf("error receiving messages: %w", err)
		case <-ctx.Done():
			break
		}
	}

	return []any{map[string]any{
		"topicId":        topicId,
		"subscriptionId": subscriptionId,
		"messages":       messages,
	}}, nil
}

// ParseParams implements tools.Tool
func (t *Tool) ParseParams(params map[string]any, claimsMap map[string]map[string]any) (tools.ParamValues, error) {
	return tools.ParseParams(t.params, params, claimsMap)
}

// Manifest returns the tool's manifest.
func (t *Tool) Manifest() tools.Manifest {
	return tools.Manifest{
		Description:  t.mcpManifest.Description,
		Parameters:   t.params.Manifest(),
		AuthRequired: t.authRequired,
	}
}

// McpManifest returns the tool's MCP manifest.
func (t *Tool) McpManifest() tools.McpManifest {
	return t.mcpManifest
}

// Authorized checks if the tool is authorized for the given services.
func (t *Tool) Authorized(verifiedAuthServices []string) bool {
	return tools.IsAuthorized(t.authRequired, verifiedAuthServices)
}
