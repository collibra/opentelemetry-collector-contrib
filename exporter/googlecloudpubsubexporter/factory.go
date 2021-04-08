// Copyright The OpenTelemetry Authors
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

package googlecloudpubsubexporter

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	// The value of "type" key in configuration.
	typeStr        = "googlecloudpubsub"
	defaultTimeout = 12 * time.Second
)

func NewFactory() component.ExporterFactory {
	return &PubsubExporterFactory{
		exporters: make(map[string]*pubsubExporter),
	}
}

type PubsubExporterFactory struct {
	exporters map[string]*pubsubExporter
}

func (factory *PubsubExporterFactory) ensureReceiver(params component.ExporterCreateParams, config config.Exporter) *pubsubExporter {
	receiver := factory.exporters[config.Name()]
	if receiver != nil {
		return receiver
	}
	rconfig := config.(*Config)
	receiver = &pubsubExporter{
		instanceName: config.Name(),
		logger:       params.Logger,
		userAgent:    strings.ReplaceAll(rconfig.UserAgent, "{{version}}", params.ApplicationStartInfo.Version),
		config:       rconfig,
	}
	factory.exporters[config.Name()] = receiver
	return receiver
}

func (factory *PubsubExporterFactory) Type() config.Type {
	return typeStr
}

// createDefaultConfig creates the default configuration for exporter.
func (factory *PubsubExporterFactory) CreateDefaultConfig() config.Exporter {
	return &Config{
		ExporterSettings: config.NewExporterSettings(typeStr),
		UserAgent:       "opentelemetry-collector-contrib {{version}}",
		TimeoutSettings: exporterhelper.TimeoutSettings{Timeout: defaultTimeout},
	}
}

func (factory *PubsubExporterFactory) CreateTracesExporter(
	_ context.Context,
	params component.ExporterCreateParams,
	cfg config.Exporter) (component.TracesExporter, error) {

	exporter := factory.ensureReceiver(params, cfg)
	exporter.tracesTopicName = fmt.Sprintf("projects/%s/topics/%s", cfg.(*Config).ProjectID ,cfg.(*Config).Traces)
	return exporter, nil
}

func (factory *PubsubExporterFactory) CreateMetricsExporter(
	_ context.Context,
	params component.ExporterCreateParams,
	cfg config.Exporter) (component.MetricsExporter, error) {

	exporter := factory.ensureReceiver(params, cfg)
	exporter.metricsTopicName = fmt.Sprintf("projects/%s/topics/%s", cfg.(*Config).ProjectID ,cfg.(*Config).Metrics)
	return exporter, nil
}

func (factory *PubsubExporterFactory) CreateLogsExporter(
	_ context.Context,
	params component.ExporterCreateParams,
	cfg config.Exporter) (component.LogsExporter, error) {
	exporter := factory.ensureReceiver(params, cfg)
	exporter.logsTopicName = fmt.Sprintf("projects/%s/topics/%s", cfg.(*Config).ProjectID ,cfg.(*Config).Logs)
	return exporter, nil
}
