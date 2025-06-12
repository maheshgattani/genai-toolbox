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
	"fmt"

	cloudpubsub "cloud.google.com/go/pubsub"
	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	"github.com/googleapis/genai-toolbox/internal/sources/pubsub"
	"github.com/googleapis/genai-toolbox/internal/tools"
)

const ToolKind = "pubsubcreatesubscription"

// Config represents the configuration for the Pub/Sub create subscription tool.
type Config struct {
	Name         string   `yaml:"name" validate:"required"`
	Kind         string   `yaml:"kind" validate:"required"`
	Source       string   `yaml:"source"`
	Description  string   `yaml:"description" validate:"required"`
	AuthRequired []string `yaml:"authRequired"`
}

// Tool implements the tools.Tool interface for creating Pub/Sub subscriptions.
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

// NewConfig creates a new configuration for the Pub/Sub create subscription tool.
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
	config.Description = "Creates a new subscription to a Cloud Pub/Sub topic"

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
		tools.NewStringParameter("subscriptionId", "The ID of the subscription to create"),
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

// Invoke creates a new subscription to a Pub/Sub topic.
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

	topic := t.source.Client().Topic(topicId)
	sub, err := t.source.Client().CreateSubscription(ctx, subscriptionId, cloudpubsub.SubscriptionConfig{
		Topic: topic,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create subscription: %w", err)
	}

	return []any{map[string]any{
		"subscriptionId": sub.ID(),
		"topicId":        topicId,
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
