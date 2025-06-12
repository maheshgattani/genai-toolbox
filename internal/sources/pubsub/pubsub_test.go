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

package pubsub

import (
	"context"
	"strings"
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/otel/trace"
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
projectId: test-project
credentialsFile: /path/to/creds.json
endpoint: localhost:8085
`,
			want: &Config{
				ProjectID:       "test-project",
				CredentialsFile: "/path/to/creds.json",
				Endpoint:        "localhost:8085",
			},
			wantErr: false,
		},
		{
			name: "missing projectId",
			yaml: `
credentialsFile: /path/to/creds.json
endpoint: localhost:8085
`,
			want:    nil,
			wantErr: true,
		},
		{
			name: "minimal config",
			yaml: `
projectId: test-project
`,
			want: &Config{
				ProjectID: "test-project",
			},
			wantErr: false,
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
				if diff := cmp.Diff(got, tt.want); diff != "" {
					t.Errorf("NewConfig() = %v, want %v, diff: %v", got, tt.want, diff)
				}
			}
		})
	}
}

func TestSourceConfigKind(t *testing.T) {
	config := &Config{}
	if got := config.SourceConfigKind(); got != SourceKind {
		t.Errorf("SourceConfigKind() = %v, want %v", got, SourceKind)
	}
}

func TestSourceKind(t *testing.T) {
	source := &Source{}
	if got := source.SourceKind(); got != SourceKind {
		t.Errorf("SourceKind() = %v, want %v", got, SourceKind)
	}
}

func TestInitialize(t *testing.T) {
	// Skip this test in CI environment
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	config := &Config{
		ProjectID: "test-project",
		Endpoint:  "localhost:8085", // Use emulator endpoint for testing
	}

	ctx := context.Background()
	tracer := trace.NewNoopTracerProvider().Tracer("test")

	source, err := config.Initialize(ctx, tracer)
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	// Verify source type
	if _, ok := source.(*Source); !ok {
		t.Errorf("Initialize() returned wrong type: %T", source)
	}

	// Clean up
	if err := source.(*Source).Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}
