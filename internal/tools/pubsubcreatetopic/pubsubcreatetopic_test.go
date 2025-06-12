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

package pubsubcreatetopic

import (
	"context"
	"strings"
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	"github.com/googleapis/genai-toolbox/internal/sources/pubsub"
	"github.com/googleapis/genai-toolbox/internal/tools"
)

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
	// Skip this test in CI environment
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

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
	// Skip this test in CI environment
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	tool := &Tool{
		source: &pubsub.Source{},
		params: tools.Parameters{
			tools.NewStringParameter("topicId", "The ID of the topic to create"),
		},
	}

	params := tools.ParamValues{
		{Name: "topicId", Value: "test-topic"},
	}

	_, err := tool.Invoke(context.Background(), params)
	if err != nil {
		t.Errorf("Invoke() error = %v", err)
	}
}

func TestParseParams(t *testing.T) {
	tool := &Tool{
		params: tools.Parameters{
			tools.NewStringParameter("topicId", "The ID of the topic to create"),
		},
	}

	params := map[string]any{
		"topicId": "test-topic",
	}

	_, err := tool.ParseParams(params, nil)
	if err != nil {
		t.Errorf("ParseParams() error = %v", err)
	}
}

func TestManifest(t *testing.T) {
	tool := &Tool{
		params: tools.Parameters{
			tools.NewStringParameter("topicId", "The ID of the topic to create"),
		},
	}

	manifest := tool.Manifest()
	if manifest.Description != "Creates a new Cloud Pub/Sub topic" {
		t.Errorf("Manifest() description = %v, want %v", manifest.Description, "Creates a new Cloud Pub/Sub topic")
	}
	if len(manifest.Parameters) != 1 {
		t.Errorf("Manifest() parameters length = %v, want %v", len(manifest.Parameters), 1)
	}
	if !tools.IsAuthorized(manifest.AuthRequired, []string{"pubsub"}) {
		t.Error("Manifest() auth required should include pubsub")
	}
}

func TestMcpManifest(t *testing.T) {
	tool := &Tool{
		params: tools.Parameters{
			tools.NewStringParameter("topicId", "The ID of the topic to create"),
		},
	}

	manifest := tool.McpManifest()
	if manifest.Name != ToolKind {
		t.Errorf("McpManifest() name = %v, want %v", manifest.Name, ToolKind)
	}
	if manifest.Description != "Creates a new Cloud Pub/Sub topic" {
		t.Errorf("McpManifest() description = %v, want %v", manifest.Description, "Creates a new Cloud Pub/Sub topic")
	}
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
