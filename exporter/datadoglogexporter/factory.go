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

package datadoglogexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	// The value of "type" key in configuration.
	typeStr        = "datadoglog"
	defaultTimeout = 12 * time.Second
)

func NewFactory() component.ExporterFactory {
	return exporterhelper.NewFactory(
		typeStr,
		createDefaultConfig,
		exporterhelper.WithLogs(createLogsExporter))
}

// createDefaultConfig creates the default configuration for exporter.
func createDefaultConfig() config.Exporter {
	return &Config{
		ExporterSettings: config.NewExporterSettings(config.NewID(typeStr)),
		TimeoutSettings:  exporterhelper.TimeoutSettings{Timeout: defaultTimeout},
	}
}

func createLogsExporter(
	_ context.Context,
	params component.ExporterCreateSettings,
	cfg config.Exporter) (component.LogsExporter, error) {

	pCfg := cfg.(*Config)

	exporter := &pubsubExporter{
		instanceName: pCfg.ID().Name(),
		logger:       params.Logger,
		config:       pCfg,
	}

	return exporterhelper.NewLogsExporter(
		cfg,
		params.Logger,
		exporter.ConsumeLogs,
		exporterhelper.WithTimeout(pCfg.TimeoutSettings),
		exporterhelper.WithRetry(pCfg.RetrySettings),
		exporterhelper.WithQueue(pCfg.QueueSettings),
		exporterhelper.WithStart(exporter.Start),
		exporterhelper.WithShutdown(exporter.Shutdown),
	)
}
