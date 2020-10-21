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

package googlecloudpubsubreceiver

import (
	"fmt"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"regexp"
)

var subscriptionMatcher = regexp.MustCompile(`projects/[a-z][a-z0-9\-]*/subscriptions/.*`)

type Config struct {
	*config.ReceiverSettings `mapstructure:"-"`

	ProjectID string `mapstructure:"project"`
	UserAgent string `mapstructure:"user_agent"`
	Endpoint  string `mapstructure:"endpoint"`
	// Only has effect if Endpoint is not ""
	UseInsecure bool `mapstructure:"use_insecure"`
	// Timeout for all API calls. If not set, defaults to 12 seconds.
	exporterhelper.TimeoutSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.

	Traces  TracesConfig  `mapstructure:"traces"`
	Metrics MetricsConfig `mapstructure:"metrics"`
	Logs    LogsConfig    `mapstructure:"logs"`

	ClientId string `mapstructure:"client_id"`
}

type TracesConfig struct {
	Subscription string `mapstructure:"subscription"`
}

func (config *TracesConfig) validate() error {
	if !subscriptionMatcher.MatchString(config.Subscription) {
		return fmt.Errorf("trace subscription '%s' is not a valide  format, use 'projects/<project_id>/subscriptions/<name>'", config.Subscription)
	}
	return nil
}

type MetricsConfig struct {
	Subscription string `mapstructure:"subscription"`
}

func (config *MetricsConfig) validate() error {
	if !subscriptionMatcher.MatchString(config.Subscription) {
		return fmt.Errorf("metric subscription '%s' is not a valide  format, use 'projects/<project_id>/subscriptions/<name>'", config.Subscription)
	}
	return nil
}

type LogsConfig struct {
	Subscription string `mapstructure:"subscription"`
	Payload      string `mapstructure:"payload"`
}

func (config *LogsConfig) validate() error {
	if !subscriptionMatcher.MatchString(config.Subscription) {
		return fmt.Errorf("log subscription '%s' is not a valide  format, use 'projects/<project_id>/subscriptions/<name>'", config.Subscription)
	}

	switch config.Payload {
	case "":
		config.Payload = "otlp"
	case "string":
	case "json":
	case "yaml":
	default:
		return fmt.Errorf("log format should be either otlp, string, json or yaml")
	}
	return nil
}
