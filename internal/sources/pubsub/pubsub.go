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
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/goccy/go-yaml"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/option"

	"github.com/googleapis/genai-toolbox/internal/sources"
)

const (
	// SourceKind is the identifier for this source type
	SourceKind = "pubsub"
)

// Config represents the configuration for a Pub/Sub source
type Config struct {
	Name      string `yaml:"name" validate:"required"`
	Kind      string `yaml:"kind" validate:"required"`
	ProjectID string `yaml:"projectId"`
	// Optional: Path to service account key file
	CredentialsFile string `yaml:"credentialsFile,omitempty"`
	// Optional: Endpoint for Pub/Sub emulator
	Endpoint string `yaml:"endpoint,omitempty"`
}

// Source implements the sources.Source interface for Cloud Pub/Sub
type Source struct {
	client    *pubsub.Client
	projectID string
}

func init() {
	sources.Register(SourceKind, NewConfig)
}

// NewConfig creates a new Pub/Sub source configuration
func NewConfig(ctx context.Context, name string, decoder *yaml.Decoder) (sources.SourceConfig, error) {
	var config Config
	config.Name = name
	config.Kind = SourceKind
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode pubsub source config: %w", err)
	}

	if config.ProjectID == "" {
		return nil, fmt.Errorf("projectId is required for pubsub source")
	}

	return &config, nil
}

// SourceConfigKind implements sources.SourceConfig
func (c *Config) SourceConfigKind() string {
	return SourceKind
}

// Initialize implements sources.SourceConfig
func (c *Config) Initialize(ctx context.Context, tracer trace.Tracer) (sources.Source, error) {
	ctx, span := sources.InitConnectionSpan(ctx, tracer, SourceKind, c.ProjectID)
	defer span.End()

	var opts []option.ClientOption
	if c.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(c.CredentialsFile))
	}
	if c.Endpoint != "" {
		opts = append(opts, option.WithEndpoint(c.Endpoint))
	}

	client, err := pubsub.NewClient(ctx, c.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	return &Source{
		client:    client,
		projectID: c.ProjectID,
	}, nil
}

// SourceKind implements sources.Source
func (s *Source) SourceKind() string {
	return SourceKind
}

// Close closes the Pub/Sub client
func (s *Source) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// Client returns the Pub/Sub client
func (s *Source) Client() *pubsub.Client {
	return s.client
}

// ProjectID returns the GCP project ID
func (s *Source) ProjectID() string {
	return s.projectID
}

// DeleteTopic deletes a Pub/Sub topic
func (s *Source) DeleteTopic(ctx context.Context, topicID string) error {
	topic := s.client.Topic(topicID)
	if err := topic.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete topic %s: %w", topicID, err)
	}
	return nil
}
