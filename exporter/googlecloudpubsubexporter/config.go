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
	"fmt"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"regexp"
)

var topicMatcher = regexp.MustCompile(`projects/[a-z][a-z0-9\-]*/topics/.*`)

type Config struct {
	*config.ExporterSettings `mapstructure:"-"`
	ProjectID                     string                   `mapstructure:"project"`
	UserAgent                     string                   `mapstructure:"user_agent"`
	Endpoint                      string                   `mapstructure:"endpoint"`
	// Only has effect if Endpoint is not ""
	UseInsecure bool `mapstructure:"use_insecure"`
	// Timeout for all API calls. If not set, defaults to 12 seconds.
	exporterhelper.TimeoutSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.

	Traces  TracesConfig  `mapstructure:"traces"`
	Metrics MetricsConfig `mapstructure:"metrics"`
	Logs    LogsConfig    `mapstructure:"logs"`
}

type TracesConfig struct {
	Topic string `mapstructure:"topic"`
}

func (config *TracesConfig) validate() error {
	if !topicMatcher.MatchString(config.Topic) {
		return fmt.Errorf("trace topic '%s' is not a valide  format, use 'projects/<project_id>/topics/<name>'", config.Topic)
	}
	return nil
}

type MetricsConfig struct {
	Topic string `mapstructure:"topic"`
}

func (config *MetricsConfig) validate() error {
	if !topicMatcher.MatchString(config.Topic) {
		return fmt.Errorf("metric topic '%s' is not a valide  format, use 'projects/<project_id>/topics/<name>'", config.Topic)
	}
	return nil
}

type LogsConfig struct {
	Topic string `mapstructure:"topic"`
}

func (config *LogsConfig) validate() error {
	if !topicMatcher.MatchString(config.Topic) {
		return fmt.Errorf("log topic '%s' is not a valide  format, use 'projects/<project_id>/topics/<name>'", config.Topic)
	}
	return nil
}
