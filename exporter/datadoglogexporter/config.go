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
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	config.ExporterSettings `mapstructure:",squash"`
	// Timeout for all API calls. If not set, defaults to 12 seconds.
	exporterhelper.TimeoutSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	exporterhelper.QueueSettings   `mapstructure:"sending_queue"`
	exporterhelper.RetrySettings   `mapstructure:"retry_on_failure"`
	// https://http-intake.logs.datadoghq.com/v1/input
	Url    string `mapstructure:"url"`
	Key    string `mapstructure:"key"`
	Source string `mapstructure:"source"`

	TagOperations   []Tag   `mapstructure:"tag_operations"`
	LabelOperations []Label `mapstructure:"label_operations"`

	ServiceDetect []DetectOperation `mapstructure:"service_detect"`
	SourceDetect  []DetectOperation `mapstructure:"source_detect"`
}

type Tag struct {
	From string `mapstructure:"from"`
	To   string `mapstructure:"to"`
}

type Label struct {
	Operation string `mapstructure:"operation"`
	From      string `mapstructure:"from"`
	To        string `mapstructure:"to"`
}

type DetectOperation struct {
	Operation string `mapstructure:"operation"`
	Name      string `mapstructure:"name"`
	Field     string `mapstructure:"field"`
	Equals    string `mapstructure:"equals"`
}
