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

package pubsubdeletetopic

import (
	"context"
	"fmt"

	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	"github.com/googleapis/genai-toolbox/internal/sources/pubsub"
	"github.com/googleapis/genai-toolbox/internal/tools"
)

const (
	// ToolKind is the identifier for this tool type
	ToolKind = "pubsubdeletetopic"
)

// Config represents the configuration for the Pub/Sub topic deletion tool
type Config struct {
	Name         string   `yaml:"name" validate:"required"`
	Kind         string   `yaml:"kind" validate:"required"`
	Source       string   `yaml:"source"`
	Description  string   `yaml:"description" validate:"required"`
	AuthRequired []string `yaml:"authRequired"`
}

// Tool implements the tools.Tool interface for deleting Pub/Sub topics
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

// NewConfig creates a new Pub/Sub topic deletion tool configuration
func NewConfig(ctx context.Context, name string, decoder *yaml.Decoder) (tools.ToolConfig, error) {
	var config Config
	config.Name = name
	config.Kind = ToolKind
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode pubsubdeletetopic tool config: %w", err)
	}

	if config.Source == "" {
		return nil, fmt.Errorf("source is required for pubsubdeletetopic tool")
	}

	return &config, nil
}

// ToolConfigKind implements tools.ToolConfig
func (c *Config) ToolConfigKind() string {
	return ToolKind
}

// Initialize implements tools.ToolConfig
func (c *Config) Initialize(sources map[string]sources.Source) (tools.Tool, error) {
	source, ok := sources[c.Source]
	if !ok {
		return nil, fmt.Errorf("source %q not found", c.Source)
	}

	pubsubSource, ok := source.(*pubsub.Source)
	if !ok {
		return nil, fmt.Errorf("source %q is not a pubsub source", c.Source)
	}

	params := tools.Parameters{
		tools.NewStringParameter("topicId", "The ID of the topic to delete"),
	}

	mcpManifest := tools.McpManifest{
		Name:        c.Name,
		Description: "Deletes a Cloud Pub/Sub topic",
		InputSchema: params.McpManifest(),
	}

	return &Tool{
		source:       pubsubSource,
		params:       params,
		Name:         c.Name,
		mcpManifest:  mcpManifest,
		authRequired: c.AuthRequired,
	}, nil
}

// Invoke implements tools.Tool
func (t *Tool) Invoke(ctx context.Context, params tools.ParamValues) ([]any, error) {
	paramsMap := params.AsMap()
	topicID, ok := paramsMap["topicId"].(string)
	if !ok {
		return nil, fmt.Errorf("topicId parameter is required and must be a string")
	}

	// Delete the topic
	if err := t.source.DeleteTopic(ctx, topicID); err != nil {
		return nil, fmt.Errorf("failed to delete topic: %w", err)
	}

	return []any{
		map[string]string{
			"message": fmt.Sprintf("Successfully deleted topic: %s", topicID),
		},
	}, nil
}

// ParseParams implements tools.Tool
func (t *Tool) ParseParams(params map[string]any, claimsMap map[string]map[string]any) (tools.ParamValues, error) {
	return tools.ParseParams(t.params, params, claimsMap)
}

// Manifest implements tools.Tool
func (t *Tool) Manifest() tools.Manifest {
	return tools.Manifest{
		Description:  "Deletes a Cloud Pub/Sub topic",
		Parameters:   t.params.Manifest(),
		AuthRequired: t.authRequired,
	}
}

// McpManifest implements tools.Tool
func (t *Tool) McpManifest() tools.McpManifest {
	return t.mcpManifest
}

// Authorized implements tools.Tool
func (t *Tool) Authorized(verifiedAuthServices []string) bool {
	return tools.IsAuthorized(t.authRequired, verifiedAuthServices)
}
