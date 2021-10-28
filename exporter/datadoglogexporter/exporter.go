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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadoglogexporter/internal/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/pdata"
	semconv "go.opentelemetry.io/collector/model/semconv/v1.6.1"
	"go.uber.org/zap"
)

const (
	// https://docs.datadoghq.com/api/latest/logs/#send-logs
	MaxBatchMessages int = 1000
	MaxBatchSize     int = 5 * 1000 * 1000 // 5 MB

	// https://docs.datadoghq.com/logs/log_configuration/attributes_naming_convention/#reserved-attributes
	MessageAttributeMessage   string = "message"
	MessageAttributeSpanID    string = "span_id"
	MessageAttributeStatus    string = "status"
	MessageAttributeTimestamp string = "timestamp"
	MessageAttributeTraceID   string = "trace_id"

	// OpenTelemetry convention extensions
	AttributeHost        string = "host"
	AttributeLogName     string = "log.name"
	AttributeServiceType string = "service.type"
)

// https://github.com/DataDog/datadog-api-client-go/blob/v1.5.0/api/v1/datadog/model_http_log_item.go#L16
type datadogMessage struct {
	Source   string      `json:"ddsource,omitempty"`
	Tags     string      `json:"ddtags,omitempty"`
	Hostname string      `json:"hostname,omitempty"`
	Message  interface{} `json:"message,omitempty"`
	Service  string      `json:"service,omitempty"`
}

func (m *datadogMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// datadogExporterBatch collects messages in batches and send those using the datadogExporter
type datadogExporterBatch struct {
	logger   *zap.Logger
	exporter *datadogExporter
	buffer   *bytes.Buffer
	count    int

	maxBatchSize     int
	maxBatchMessages int
}

// Append a datadog message to the batch. This method will automatically call WriteBatch once the batch
// reaches one of those limits:
// - batch payload size of MaxBatchSize
// - batch message count of MaxBatchMessages
func (b *datadogExporterBatch) Append(message *datadogMessage) {
	rawMessage, err := message.Marshal()
	if err != nil {
		b.logger.Warn("failed to marshal datadog message", zap.Error(err))
	}

	// Batch is full, write it (buffer size + message size + ',' + ']')
	if b.buffer.Len()+len(rawMessage)+2 > b.maxBatchSize {
		b.logger.Debug("Reached max batch size, writing batch", zap.Int("max_batch_size", b.maxBatchSize), zap.Int("batch_size", b.buffer.Len()))
		b.WriteBatch()
	} else if b.count >= b.maxBatchMessages {
		b.logger.Debug("Reached max messages per batch, writing batch", zap.Int("max_batch_messages", b.maxBatchMessages), zap.Int("batch_messages", b.count))
		b.WriteBatch()
	}

	b.appendToBatch(rawMessage)
}

func (b *datadogExporterBatch) appendToBatch(message []byte) {
	if len(message) == 0 {
		return
	}
	if b.buffer.Len() == 0 {
		b.buffer.WriteByte('[')
	} else {
		b.buffer.WriteByte(',')
	}
	b.buffer.Write(message)
	b.count++
}

// WriteBatch write the batch using the datadogExporter.
// This is a noop if no messages have been appended.
func (b *datadogExporterBatch) WriteBatch() error {
	if b.buffer.Len() == 0 {
		return nil
	}
	b.buffer.WriteByte(']')
	err := b.exporter.send(b.buffer)
	b.buffer.Reset()
	b.count = 0
	return err
}

// datadogExporter is a wrapper around the DataDog Log Intake API
type datadogExporter struct {
	logger    *zap.Logger
	buildInfo component.BuildInfo
	config    *Config

	client *client.DatadogClient
}

func (*datadogExporter) Name() string {
	return typeStr
}

func (ex *datadogExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: false,
	}
}

func (ex *datadogExporter) Start(context.Context, component.Host) error {
	userAgent := fmt.Sprintf("%s/%s", ex.buildInfo.Command, ex.buildInfo.Version)
	ex.client = client.NewClient(ex.config.APIKey, userAgent)
	return nil
}

func (ex *datadogExporter) Shutdown(context.Context) error {
	if ex.client != nil {
		ex.client.Close()
		ex.client = nil
	}
	return nil
}

func (ex *datadogExporter) ConsumeLogs(_ context.Context, logs pdata.Logs) error {
	batch := ex.newBatch()

	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		resourceLogs := logs.ResourceLogs().At(i)
		instrumentationLibraryLogs := resourceLogs.InstrumentationLibraryLogs()
		for ii := 0; ii < instrumentationLibraryLogs.Len(); ii++ {
			logs := instrumentationLibraryLogs.At(ii)
			instrumentationLibraryLog := logs.Logs()
			for li := 0; li < instrumentationLibraryLog.Len(); li++ {
				msg := ex.toMessage(resourceLogs, logs, instrumentationLibraryLog.At(li))
				if msg != nil {
					batch.Append(msg)
				}
			}
		}
	}

	return batch.WriteBatch()
}

func (ex *datadogExporter) newBatch() *datadogExporterBatch {
	return &datadogExporterBatch{
		logger:           ex.logger.Named("batch"),
		exporter:         ex,
		buffer:           bytes.NewBuffer([]byte{}),
		count:            0,
		maxBatchSize:     MaxBatchSize,
		maxBatchMessages: MaxBatchMessages,
	}
}

func (ex *datadogExporter) send(buffer *bytes.Buffer) error {
	request, err := http.NewRequest("POST", ex.config.URL, buffer)
	if err != nil {
		return err
	}

	request.Header.Set(client.HeaderContentType, client.ContentTypeApplicationJSON)

	response, err := ex.client.Do(request)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		return fmt.Errorf("send failed with status code %v", response.StatusCode)
	}
	return nil
}

func (ex *datadogExporter) toMessage(resourceLogs pdata.ResourceLogs, instrLogs pdata.InstrumentationLibraryLogs, record pdata.LogRecord) *datadogMessage {
	var messageMap map[string]interface{}
	switch record.Body().Type() {
	case pdata.AttributeValueTypeMap:
		messageMap = record.Body().MapVal().AsRaw()
	case pdata.AttributeValueTypeString:
		messageMap = ex.getMessageMapFromText(record.Body().StringVal())
	}
	if messageMap != nil {
		return ex.buildMessage(resourceLogs, instrLogs, record, messageMap)
	}
	return nil
}

func (ex *datadogExporter) getMessageMapFromText(messageText string) map[string]interface{} {
	var messageMap map[string]interface{}
	err := json.Unmarshal([]byte(messageText), &messageMap)
	if err != nil {
		ex.logger.Debug("Failed to parse string message as JSON", zap.Error(err))
		messageMap = map[string]interface{}{MessageAttributeMessage: messageText}
	}
	return messageMap
}

func (ex *datadogExporter) buildMessage(resourceLogs pdata.ResourceLogs, instrLogs pdata.InstrumentationLibraryLogs, record pdata.LogRecord, messageMap map[string]interface{}) *datadogMessage {
	labels := extractLabels(resourceLogs, instrLogs, record)
	ex.completeMessage(messageMap, record)
	return &datadogMessage{
		Hostname: ex.getHostname(labels),
		Service:  ex.getService(labels),
		Source:   ex.getSource(labels),
		Tags:     ex.getTags(labels),
		Message:  messageMap,
	}
}

func (ex *datadogExporter) getHostname(labels map[string]string) string {
	if hostname, found := getFirstNotEmptyLabel(labels, []string{semconv.AttributeHostName, AttributeHost}); found {
		return hostname
	}
	return ""
}

func (ex *datadogExporter) getSource(labels map[string]string) string {
	if source, found := getFirstNotEmptyLabel(labels, []string{AttributeServiceType, semconv.AttributeServiceName, AttributeLogName}); found {
		return source
	}
	return ""
}

func (ex *datadogExporter) getService(labels map[string]string) string {
	if service, found := getFirstNotEmptyLabel(labels, []string{semconv.AttributeServiceName, AttributeLogName, semconv.AttributeK8SContainerName, semconv.AttributeK8SPodName}); found {
		return service
	}
	return ""
}

func (ex *datadogExporter) getTags(labels map[string]string) string {
	var tags []string
	for key, value := range labels {
		if value != "" {
			tags = append(tags, fmt.Sprintf("%s:%s", key, value))
		} else {
			tags = append(tags, key)
		}
	}
	if len(tags) == 0 {
		return ""
	}
	return strings.Join(tags, ",")
}

func (ex *datadogExporter) completeMessage(messageMap map[string]interface{}, record pdata.LogRecord) {
	if messageMap[MessageAttributeTimestamp] == nil {
		messageMap[MessageAttributeTimestamp] = record.Timestamp().AsTime().Format("2006-01-02T15:04:05.000Z0700")
	}
	if messageMap[MessageAttributeStatus] == nil && record.SeverityText() != "" {
		messageMap[MessageAttributeStatus] = record.SeverityText()
	}
	if !record.TraceID().IsEmpty() {
		messageMap[MessageAttributeTraceID] = record.TraceID().HexString()
	}
	if !record.SpanID().IsEmpty() {
		messageMap[MessageAttributeSpanID] = record.SpanID().HexString()
	}
}

func extractLabels(logs pdata.ResourceLogs, il pdata.InstrumentationLibraryLogs, logRecord pdata.LogRecord) map[string]string {
	labels := map[string]string{}
	logs.Resource().Attributes().Range(func(k string, v pdata.AttributeValue) bool {
		labels[k] = v.StringVal()
		return true
	})
	// Priority to attributes
	logRecord.Attributes().Range(func(k string, v pdata.AttributeValue) bool {
		labels[k] = v.StringVal()
		return true
	})
	if il.InstrumentationLibrary().Name() != "" {
		labels["opentelemetry.org/instrumentation/name"] = il.InstrumentationLibrary().Name()
	}
	if il.InstrumentationLibrary().Version() != "" {
		labels["opentelemetry.org/instrumentation/version"] = il.InstrumentationLibrary().Version()
	}
	return labels
}

// getFirstNotEmptyLabel finds the first not empty label in the provided labels from the list of keys
// returns the value and a bool to know if a not empty value has been found
func getFirstNotEmptyLabel(labels map[string]string, keys []string) (string, bool) {
	for i := range keys {
		key := keys[i]
		if value, found := labels[key]; found && value != "" {
			return value, true
		}
	}
	return "", false
}
