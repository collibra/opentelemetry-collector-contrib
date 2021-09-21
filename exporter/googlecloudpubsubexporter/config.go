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
	"regexp"

	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

var topicMatcher = regexp.MustCompile(`^projects/[a-z][a-z0-9\-]*/topics/`)

type Config struct {
	config.ExporterSettings `mapstructure:",squash"`
	// Timeout for all API calls. If not set, defaults to 12 seconds.
	exporterhelper.TimeoutSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	exporterhelper.QueueSettings   `mapstructure:"sending_queue"`
	exporterhelper.RetrySettings   `mapstructure:"retry_on_failure"`
	// Google Cloud Project ID where the Pubsub client will connect to
	ProjectID string `mapstructure:"project"`
	// User agent that will be used by the Pubsub client to connect to the service
	UserAgent string `mapstructure:"user_agent"`
	// Override of the Pubsub endpoint
	Endpoint string `mapstructure:"endpoint"`
	// Only has effect if Endpoint is not ""
	Insecure bool `mapstructure:"insecure"`

	// The fully qualified resource name of the Pubsub topic
	Topic string `mapstructure:"topic"`
	// Lock down the encoding of the payload, leave empty for attribute based detection
	Compression string `mapstructure:"compression"`
}

func (config *Config) validate() error {
	if !topicMatcher.MatchString(config.Topic) {
		return fmt.Errorf("topic '%s' is not a valid format, use 'projects/<project_id>/topics/<name>'", config.Topic)
	}
	switch config.Compression {
	case "":
	case "gzip":
	default:
		return fmt.Errorf("if specified, compression should be gzip")
	}
	return nil
}

func (config *Config) parseCompression() Compression {
	switch config.Compression {
	case "gzip":
		return GZip
	}
	return Uncompressed
}
