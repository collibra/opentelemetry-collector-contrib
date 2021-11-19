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
	"errors"

	semconv "go.opentelemetry.io/collector/model/semconv/v1.6.1"

	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	AttributeHost               string = "host"
	AttributeLogName            string = "log.name"
	AttributeServiceType        string = "service.type"
	AttributeFileName           string = "file.name"
	AttributeFileNameUnderscore string = "file_name"
)

var (
	errUnsetURL    = errors.New("URL is not set")
	errUnsetAPIKey = errors.New("APIKey is not set")

	DefaultHostnameFrom = []string{semconv.AttributeHostName, AttributeHost}
	DefaultSourceFrom   = []string{AttributeServiceType, semconv.AttributeServiceName, AttributeLogName}
	DefaultServiceFrom  = []string{semconv.AttributeServiceName, AttributeLogName, semconv.AttributeK8SContainerName, semconv.AttributeK8SPodName}
	DefaultFileNameFrom = []string{AttributeFileName, AttributeFileNameUnderscore}
)

type Config struct {
	config.ExporterSettings `mapstructure:",squash"`
	// Timeout for all API calls. If not set, defaults to 12 seconds.
	exporterhelper.TimeoutSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	exporterhelper.QueueSettings   `mapstructure:"sending_queue"`
	exporterhelper.RetrySettings   `mapstructure:"retry_on_failure"`

	// https://docs.datadoghq.com/api/latest/logs/#send-logs
	URL string `mapstructure:"url"`
	// https://docs.datadoghq.com/account_management/api-app-keys/
	APIKey string `mapstructure:"api_key"`

	HostnameFrom    []string `mapstructure:"hostname_from"`
	SourceFrom      []string `mapstructure:"source_from"`
	ServiceNameFrom []string `mapstructure:"service_from"`
	FileNameFrom    []string `mapstructure:"filename_from"`
}

func (config Config) Validate() error {
	if config.URL == "" {
		return errUnsetURL
	}
	if config.APIKey == "" {
		return errUnsetAPIKey
	}
	return nil
}

func (config *Config) InitDefaults() {
	if len(config.HostnameFrom) == 0 {
		config.HostnameFrom = DefaultHostnameFrom
	}
	if len(config.SourceFrom) == 0 {
		config.SourceFrom = DefaultSourceFrom
	}
	if len(config.ServiceNameFrom) == 0 {
		config.ServiceNameFrom = DefaultServiceFrom
	}
	if len(config.FileNameFrom) == 0 {
		config.FileNameFrom = DefaultFileNameFrom
	}
}
