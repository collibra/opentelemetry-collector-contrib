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

package testdata

import (
	"go.opentelemetry.io/collector/model/otlp"
	"go.opentelemetry.io/collector/model/pdata"
)

func CreateTraceExport() []byte {
	out := pdata.NewTraces()
	resources := out.ResourceSpans()
	resources.Resize(1)
	resource := resources.At(0)
	libs := resource.InstrumentationLibrarySpans()
	libs.Resize(1)
	spans := libs.At(0).Spans()
	spans.Resize(1)
	span := spans.At(0)
	span.SetName("test")
	data, _ := otlp.NewProtobufTracesMarshaler().MarshalTraces(out)
	return data
}

func CreateMetricExport() []byte {
	out := pdata.NewMetrics()
	resources := out.ResourceMetrics()
	resources.Resize(1)
	resource := resources.At(0)
	libs := resource.InstrumentationLibraryMetrics()
	libs.Resize(1)
	metrics := libs.At(0).Metrics()
	metrics.Resize(1)
	metric := metrics.At(0)
	metric.SetName("test")
	data, _ := otlp.NewProtobufMetricsMarshaler().MarshalMetrics(out)
	return data
}

func CreateLogExport() []byte {
	out := pdata.NewLogs()
	resources := out.ResourceLogs()
	resources.Resize(1)
	resource := resources.At(0)
	libs := resource.InstrumentationLibraryLogs()
	libs.Resize(1)
	logs := libs.At(0).Logs()
	logs.Resize(1)
	log := logs.At(0)
	log.SetName("test")
	data, _ := otlp.NewProtobufLogsMarshaler().MarshalLogs(out)
	return data
}

func CreateTextExport() []byte {
	return []byte("this is text")
}
