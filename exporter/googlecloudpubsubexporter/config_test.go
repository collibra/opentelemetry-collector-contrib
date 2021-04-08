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
	"go.opentelemetry.io/collector/component/componenttest"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/configtest"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

func TestLoadConfig(t *testing.T) {
	factories, err := componenttest.NopFactories()
	assert.Nil(t, err)

	factory := NewFactory()
	factories.Exporters[config.Type(typeStr)] = factory
	cfg, err := configtest.LoadConfigFile(
		t, path.Join(".", "testdata", "config.yaml"), factories,
	)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, len(cfg.Exporters), 2)

	r0 := cfg.Exporters["googlecloudpubsub"]
	assert.Equal(t, r0, factory.CreateDefaultConfig())

	r1 := cfg.Exporters["googlecloudpubsub/customname"].(*Config)
	assert.Equal(t, r1,
		&Config{
			ExporterSettings: &config.ExporterSettings{TypeVal: config.Type(typeStr), NameVal: "googlecloudpubsub/customname"},
			ProjectID:        "my-project",
			UserAgent:        "opentelemetry-collector-contrib {{version}}",
			Endpoint:         "test-endpoint",
			UseInsecure:      true,
			TimeoutSettings: exporterhelper.TimeoutSettings{
				Timeout: 20 * time.Second,
			},
			Traces: TracesConfig{
				Topic: "otlp-traces",
			},
			Metrics: MetricsConfig{
				Topic: "otlp-metrics",
			},
			Logs: LogsConfig{
				Topic: "otlp-logs",
			},
		})
}
