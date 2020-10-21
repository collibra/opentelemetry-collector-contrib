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
	"go.opentelemetry.io/collector/config"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcheck"
	"go.uber.org/zap"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, configcheck.ValidateConfig(cfg))
}

func TestType(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, config.Type("googlecloudpubsub"), factory.Type())
}

func TestCreateTracesExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	eCfg := cfg.(*Config)
	eCfg.Endpoint = "http://testing.invalid"

	te, err := factory.CreateTracesExporter(
		context.Background(),
		component.ExporterCreateParams{Logger: zap.NewNop()},
		eCfg,
	)
	assert.NoError(t, err)
	assert.NotNil(t, te, "failed to create trace exporter")
}

func TestCreateMetricsExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	eCfg := cfg.(*Config)
	eCfg.Endpoint = "http://testing.invalid"

	me, err := factory.CreateMetricsExporter(
		context.Background(),
		component.ExporterCreateParams{Logger: zap.NewNop()},
		eCfg,
	)
	assert.NoError(t, err)
	assert.NotNil(t, me, "failed to create metrics exporter")
}

func TestLogsCreateExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	eCfg := cfg.(*Config)
	eCfg.Endpoint = "http://testing.invalid"

	me, err := factory.CreateLogsExporter(
		context.Background(),
		component.ExporterCreateParams{Logger: zap.NewNop()},
		eCfg,
	)
	assert.NoError(t, err)
	assert.NotNil(t, me, "failed to create logs exporter")
}

func TestCEnsureExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	eCfg := cfg.(*Config)
	eCfg.Endpoint = "http://testing.invalid"

	te, err := factory.CreateTracesExporter(
		context.Background(),
		component.ExporterCreateParams{Logger: zap.NewNop()},
		eCfg,
	)
	assert.NoError(t, err)
	me, err := factory.CreateMetricsExporter(
		context.Background(),
		component.ExporterCreateParams{Logger: zap.NewNop()},
		eCfg,
	)
	assert.NoError(t, err)
	assert.Equal(t, te, me)
	le, err := factory.CreateLogsExporter(
		context.Background(),
		component.ExporterCreateParams{Logger: zap.NewNop()},
		eCfg,
	)
	assert.NoError(t, err)
	assert.Equal(t, me, le)
}
