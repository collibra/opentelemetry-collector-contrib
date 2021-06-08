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
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
)

const name = "googlecloudpubsub"

// pubsubExporter is a wrapper struct of OT cloud trace exporter
type pubsubExporter struct {
	instanceName string
	logger       *zap.Logger

	client *http.Client

	cancel context.CancelFunc
	wg     sync.WaitGroup

	//
	userAgent string
	config    *Config
	//
}

func (*pubsubExporter) Name() string {
	return name
}

type Encoding int

func (ex *pubsubExporter) Start(ctx context.Context, _ component.Host) error {
	//
	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: false,
	}
	ex.client = &http.Client{Transport: tr}

	return nil
}

func (ex *pubsubExporter) Shutdown(context.Context) error {
	if ex.client != nil {
		ex.client.CloseIdleConnections()
		ex.client = nil
	}
	return nil
}

func (ex *pubsubExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: false,
	}
}

type DataDogMessage struct {
	DDSource  string      `json:"ddsource"`
	DDService string      `json:"ddservice"`
	DDTags    string      `json:"ddtags"`
	Message   interface{} `json:"message"`
}

func (ex *pubsubExporter) treeWalker(attrMap pdata.AttributeMap) map[string]interface{} {
	out := map[string]interface{}{}

	attrMap.Range(func(key string, value pdata.AttributeValue) bool {
		switch value.Type() {
		case pdata.AttributeValueTypeMap:
			out[key] = ex.treeWalker(value.MapVal())
		case pdata.AttributeValueTypeString:
			out[key] = value.StringVal()
		case pdata.AttributeValueTypeBool:
			out[key] = value.BoolVal()
		case pdata.AttributeValueTypeDouble:
			out[key] = value.DoubleVal()
		case pdata.AttributeValueTypeInt:
			out[key] = value.IntVal()
		}
		return true
	})
	return out
}

func (ex *pubsubExporter) logToJson(record pdata.LogRecord) map[string]interface{} {
	body := record.Body()
	return ex.treeWalker(body.MapVal())
}

func (ex *pubsubExporter) marshalMap(resourceLogs pdata.ResourceLogs, instrLogs pdata.InstrumentationLibraryLogs, record pdata.LogRecord) ([]byte, error) {
	out := ex.logToJson(record)
	ex.handleConventions(resourceLogs, instrLogs, record, out)
	return json.Marshal(out)
}

func (ex *pubsubExporter) handleConventions(resourceLogs pdata.ResourceLogs, instrLogs pdata.InstrumentationLibraryLogs, record pdata.LogRecord, out map[string]interface{}) {
	ex.addTimestamp(record, out)
	ex.addSeverity(record, out)
	ex.addLabels(resourceLogs, instrLogs, record, out)
	ex.addTraceInfo(record, out)
	ex.labelOperations(out)
	ex.normalize(out)
	ex.reservedAttributes(out)
	ex.tags(out)
}

func (ex *pubsubExporter) marshal(resourceLogs pdata.ResourceLogs, instrLogs pdata.InstrumentationLibraryLogs, record pdata.LogRecord) []byte {
	var marshal []byte
	switch record.Body().Type() {
	case pdata.AttributeValueTypeMap:
		marshal, _ = ex.marshalMap(resourceLogs, instrLogs, record)
	case pdata.AttributeValueTypeString:
		marshal, _ = ex.marshalText(resourceLogs, instrLogs, record)
	}
	return marshal
}

func (ex *pubsubExporter) marshalText(resourceLogs pdata.ResourceLogs, instrLogs pdata.InstrumentationLibraryLogs, record pdata.LogRecord) ([]byte, error) {
	message := record.Body().StringVal()

	var out map[string]interface{}
	err := json.Unmarshal([]byte(message), &out)
	if err != nil {
		out = map[string]interface{}{
			"message": message,
		}
	}

	ex.handleConventions(resourceLogs, instrLogs, record, out)
	return json.Marshal(out)
}

func (ex *pubsubExporter) addToBuffer(resourceLogs pdata.ResourceLogs, instrLogs pdata.InstrumentationLibraryLogs, record pdata.LogRecord, buffer *bytes.Buffer) error {
	if buffer.Len() == 0 {
		buffer.WriteByte('[')
	} else {
		buffer.WriteByte(',')
	}
	buffer.Write(ex.marshal(resourceLogs, instrLogs, record))

	if buffer.Len() > 4000000 {
		return ex.finishBuffer(buffer)
	}
	return nil
}

func (ex *pubsubExporter) finishBuffer(buffer *bytes.Buffer) error {
	if buffer.Len() == 0 {
		return nil
	}
	buffer.WriteByte(']')

	request, _ := http.NewRequest("POST", ex.config.Url, buffer)
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("DD-API-KEY", ex.config.Key)
	//request.Header.Add("DD_APP_KEY", ex.config.Key)

	response, err := ex.client.Do(request)
	buffer.Reset()
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		return errors.New("status is not 200")
	}
	return nil
}

func (ex *pubsubExporter) ConsumeLogs(ctx context.Context, td pdata.Logs) error {

	buffer := bytes.NewBuffer([]byte{})

	for i := 0; i < td.ResourceLogs().Len(); i++ {
		resourceLogs := td.ResourceLogs().At(i)
		instrumentationLibraryLogs := resourceLogs.InstrumentationLibraryLogs()
		for ii := 0; ii < instrumentationLibraryLogs.Len(); ii++ {
			logs := instrumentationLibraryLogs.At(ii)
			instrumentationLibraryLog := logs.Logs()
			for li := 0; li < instrumentationLibraryLog.Len(); li++ {
				err := ex.addToBuffer(resourceLogs, logs, instrumentationLibraryLog.At(li), buffer)
				if err != nil {
					return err
				}
			}
		}
	}
	return ex.finishBuffer(buffer)
}

func (ex *pubsubExporter) addLabels(logs pdata.ResourceLogs, il pdata.InstrumentationLibraryLogs, logRecord pdata.LogRecord, out map[string]interface{}) {
	logRecord.Attributes().Range(func(k string, v pdata.AttributeValue) bool {
		out[k] = v.StringVal()
		return true
	})
	logs.Resource().Attributes().Range(func(k string, v pdata.AttributeValue) bool {
		out[k] = v.StringVal()
		return true
	})
	if il.InstrumentationLibrary().Name() != "" {
		out["instrumentation.name"] = il.InstrumentationLibrary().Name()
	}
	if il.InstrumentationLibrary().Version() != "" {
		out["instrumentation.version"] = il.InstrumentationLibrary().Version()
	}
}

func (ex *pubsubExporter) addSeverity(logRecord pdata.LogRecord, out map[string]interface{}) {
	if out["status"] == nil {
		out["status"] = logRecord.SeverityText()
	}
}

func (ex *pubsubExporter) addTimestamp(logRecord pdata.LogRecord, out map[string]interface{}) {
	if out["timestamp"] == nil {
		out["timestamp"] = logRecord.Timestamp().AsTime().Format("2006-01-02T15:04:05.000Z0700")
	}
}

func (ex *pubsubExporter) addTraceInfo(logRecord pdata.LogRecord, out map[string]interface{}) {
	if !logRecord.TraceID().IsEmpty() {
		out["trace_id"] = logRecord.TraceID().HexString()
	}
	if !logRecord.SpanID().IsEmpty() {
		out["span_id"] = logRecord.SpanID().HexString()
	}
}

func (ex *pubsubExporter) tags(out map[string]interface{}) {
	var tags []string

	for _, tagOperation := range ex.config.TagOperations {
		value := out[tagOperation.From]
		if value != nil {
			tags = append(tags, fmt.Sprintf("%s:%s", tagOperation.To, value))
		}
	}
	if len(tags) == 0 {
		return
	}
	out["ddtags"] = fmt.Sprintf(strings.Join(tags, ","))
}

func (ex *pubsubExporter) labelOperations(out map[string]interface{}) {
	for _, labelOperation := range ex.config.LabelOperations {
		switch labelOperation.Operation {
		case "move":
			move(out, labelOperation.From, labelOperation.To)
		case "copy":
			copy(out, labelOperation.From, labelOperation.To)
		default:
		}
	}
}

//func (ex *pubsubExporter) serviceDetect(out map[string]interface{}) {
//	for _, serviceDetect := range ex.config.ServiceDetect {
//		if out[serviceDetect.Field] == serviceDetect.Equals {
//			out["service"] = serviceDetect.Name
//			return
//		}
//	}
//}
//

func (ex *pubsubExporter) reservedAttributes(out map[string]interface{}) {
	ex.detectionRules(out, ex.config.ServiceDetect, "service")
	ex.detectionRules(out, ex.config.SourceDetect, "ddsource")
}
func (ex *pubsubExporter) detectionRules(out map[string]interface{}, operations []DetectOperation, name string) {
	if out[name] != nil {
		return
	}

	for _, operation := range operations {
		if operation.Operation == "default" {
			out[name] = operation.Name
			return
		}
		value := out[operation.Field]
		if len(operation.Equals) > 0 {
			if operation.Equals == value {
				switch operation.Operation {
				case "replace":
					delete(out, operation.Field)
					out[name] = operation.Name
					return
				case "copy":
					out[name] = operation.Name
					return
				}
			}
		} else if value != nil {
			switch operation.Operation {
			case "replace":
				sv, ok := value.(string)
				if ok {
					delete(out, operation.Field)
					out[name] = sv
					return
				}
			case "copy":
				sv, ok := value.(string)
				if ok {
					out[name] = sv
					return
				}
			}
		}
	}
}

func move(out map[string]interface{}, from string, to string) {
	if out[to] != nil {
		return
	}
	copy(out, from, to)
	delete(out, from)
}

func copy(out map[string]interface{}, from string, to string) {
	if out[to] != nil {
		return
	}
	value := out[from]
	if value != nil {
		out[to] = value
	}
}

// https://docs.datadoghq.com/logs/processing/attributes_naming_convention/#reserved-attributes
func (ex *pubsubExporter) normalize(out map[string]interface{}) {
	move(out, "logger_name", "logger.name")
	move(out, "thread_name", "logger.thread_name")
	move(out, "service.name", "service")
	move(out, "service.version", "version")
	copy(out, "host.name", "hostname")
}
