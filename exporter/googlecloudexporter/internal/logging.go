// Copyright 2019, OpenTelemetry Authors
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

package internal

import (
	"context"
	"fmt"
	"strings"

	cloudlogging "cloud.google.com/go/logging/apiv2"
	"go.opentelemetry.io/collector/model/pdata"
	monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	loggingpb "google.golang.org/genproto/googleapis/logging/v2"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GoogleLogging struct {
	ProjectID string
	LogName   string
	client    *cloudlogging.Client
}

func NewGoogleLogging(ctx context.Context, projectID string) *GoogleLogging {
	client, _ := cloudlogging.NewClient(ctx)

	return &GoogleLogging{
		ProjectID: projectID,
		client:    client,
	}
}

type GoogleLoggingBatch struct {
	exporter   *GoogleLogging
	logEntries []*loggingpb.LogEntry
}

func (b *GoogleLoggingBatch) Append(rl []*loggingpb.LogEntry) {
	b.logEntries = append(b.logEntries, rl...)

	fmt.Printf("Append %v\n", len(b.logEntries))
}

func (b *GoogleLoggingBatch) WriteBatch() {
	fmt.Printf("WriteBatch %v\n", len(b.logEntries))
	request := loggingpb.WriteLogEntriesRequest{
		LogName: b.exporter.LogName,
		Entries: b.logEntries,
	}

	response, err := b.exporter.client.WriteLogEntries(
		context.TODO(),
		&request,
	)
	_ = response
	fmt.Printf("%v\n", err)
}

func (gl *GoogleLogging) NewBatch() *GoogleLoggingBatch {
	fmt.Printf("NewBatch %v\n", 0)
	return &GoogleLoggingBatch{
		exporter:   gl,
		logEntries: make([]*loggingpb.LogEntry, 0),
	}
}

func (gl *GoogleLogging) ToLogEntries(logs pdata.ResourceLogs) []*loggingpb.LogEntry {
	il := logs.InstrumentationLibraryLogs()
	logEnties := make([]*loggingpb.LogEntry, 0)
	for i := 0; i < il.Len(); i++ {
		l := il.At(i)
		logSlice := l.Logs()
		for j := 0; j < logSlice.Len(); j++ {
			logEnties = append(logEnties, gl.ToLogEntry(logs, l, logSlice.At(j)))
		}
		//rl := te.lexporter.ToLogEntries(l.)
		//logEntries = append(logEntries, rl...)
	}

	logs.InstrumentationLibraryLogs()

	return logEnties

}

func (gl *GoogleLogging) addBodyToPayload(entry *loggingpb.LogEntry, value pdata.AttributeValue) error {
	switch value.Type() {
	case pdata.AttributeValueTypeString:
		entry.Payload = &loggingpb.LogEntry_TextPayload{
			TextPayload: value.StringVal(),
		}
		break
	case pdata.AttributeValueTypeInt:
		break
	case pdata.AttributeValueTypeDouble:
		break
	case pdata.AttributeValueTypeBool:
		break
	case pdata.AttributeValueTypeMap:
		json, _ := gl.attributeMapToStruct(value.MapVal())
		entry.Payload = &loggingpb.LogEntry_JsonPayload{
			JsonPayload: json,
		}
		break
	case pdata.AttributeValueTypeArray:
		break
	case pdata.AttributeValueTypeNull:
	}
	return nil
}

func (gl *GoogleLogging) addTraceToLogEntry(entry *loggingpb.LogEntry, logRecord pdata.LogRecord) {
	traceID := logRecord.TraceID()
	if !traceID.IsEmpty() {
		entry.Trace = fmt.Sprintf("projects/%s/traces/%s", gl.ProjectID, traceID.HexString())
	}
	spanID := logRecord.SpanID()
	if !spanID.IsEmpty() {
		entry.SpanId = spanID.HexString()
	}
	flags := logRecord.Flags()
	if flags&0x00000001 == 0x00000001 {
		entry.TraceSampled = true
	}
}

func (gl *GoogleLogging) attributeMapToStruct(val pdata.AttributeMap) (*structpb.Struct, error) {
	entries := make(map[string]interface{}, val.Len())
	val.Range(func(k string, v pdata.AttributeValue) bool {
		entries[k] = v.StringVal()
		return true
	})
	return structpb.NewStruct(entries)
}

func (gl *GoogleLogging) ExtractResource(logs pdata.ResourceLogs, logRecord pdata.LogRecord) *monitoredres.MonitoredResource {

	monitoredResource := &monitoredres.MonitoredResource{Type: "global"}

	value, ok := logRecord.Attributes().Get("file_name")
	if ok && "dgc.jsonl" == value.StringVal() {
		monitoredResource.Type = "generic_task"
		monitoredResource.Labels = map[string]string{
			"project_id": "collibra-telemetry",
			"job":        "dgc",
		}
		zone, _ := logs.Resource().Attributes().Get("cloud.availability_zone")
		accountId, _ := logs.Resource().Attributes().Get("cloud.account.id")
		if ok {
			monitoredResource.Labels["location"] = fmt.Sprintf("aws:%s:%s", accountId.StringVal(), zone.StringVal())
		}
		envName, _ := logs.Resource().Attributes().Get("collibra.instance.environment_name")
		if ok {
			monitoredResource.Labels["namespace"] = envName.StringVal()
		}
		value, ok = logs.Resource().Attributes().Get("host.id")
		if ok {
			monitoredResource.Labels["task_id"] = value.StringVal()
		}
	}

	return monitoredResource
}

func (gl *GoogleLogging) ToLabels(logs pdata.ResourceLogs, il pdata.InstrumentationLibraryLogs, logRecord pdata.LogRecord) map[string]string {
	labels := map[string]string{}
	logRecord.Attributes().Range(func(k string, v pdata.AttributeValue) bool {
		labels[k] = v.StringVal()
		return true
	})
	logs.Resource().Attributes().Range(func(k string, v pdata.AttributeValue) bool {
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

func (gl *GoogleLogging) ToLogEntry(logs pdata.ResourceLogs, il pdata.InstrumentationLibraryLogs, logRecord pdata.LogRecord) *loggingpb.LogEntry {

	monitoredResource := gl.ExtractResource(logs, logRecord)
	entry := &loggingpb.LogEntry{
		Timestamp: &timestamppb.Timestamp{
			Seconds: logRecord.Timestamp().AsTime().Unix(),
			Nanos:   int32(logRecord.Timestamp().AsTime().Nanosecond()),
		},
		LogName:  gl.LogName,
		Labels:   gl.ToLabels(logs, il, logRecord),
		Resource: monitoredResource,
	}
	fileName, ok := logRecord.Attributes().Get("file_name")
	if ok {
		gl.LogName = fmt.Sprintf("projects/collibra-telemetry/logs/%s", fileName.StringVal())
	} else {
		fmt.Printf("dhu?")
	}

	gl.addBodyToPayload(entry, logRecord.Body())
	gl.addTraceToLogEntry(entry, logRecord)
	switch strings.ToUpper(logRecord.SeverityText()) {
	case "TRACE":
		entry.Severity = ltype.LogSeverity_DEFAULT
	case "TRACE2":
		entry.Severity = ltype.LogSeverity_DEFAULT
	case "TRACE3":
		entry.Severity = ltype.LogSeverity_DEFAULT
	case "TRACE4":
		entry.Severity = ltype.LogSeverity_DEFAULT
	case "DEBUG":
		entry.Severity = ltype.LogSeverity_DEBUG
	case "DEBUG2":
		entry.Severity = ltype.LogSeverity_DEBUG
	case "DEBUG3":
		entry.Severity = ltype.LogSeverity_DEBUG
	case "DEBUG4":
		entry.Severity = ltype.LogSeverity_DEBUG
	case "INFO":
		entry.Severity = ltype.LogSeverity_INFO
	case "INFO2":
		entry.Severity = ltype.LogSeverity_INFO
	case "INFO3":
		entry.Severity = ltype.LogSeverity_INFO
	case "INFO4":
		entry.Severity = ltype.LogSeverity_INFO
	case "WARN":
		entry.Severity = ltype.LogSeverity_WARNING
	case "WARN2":
		entry.Severity = ltype.LogSeverity_WARNING
	case "WARN3":
		entry.Severity = ltype.LogSeverity_WARNING
	case "WARN4":
		entry.Severity = ltype.LogSeverity_WARNING
	case "ERROR":
		entry.Severity = ltype.LogSeverity_ERROR
	case "ERROR2":
		entry.Severity = ltype.LogSeverity_ERROR
	case "ERROR3":
		entry.Severity = ltype.LogSeverity_ERROR
	case "ERROR4":
		entry.Severity = ltype.LogSeverity_ERROR
	case "FATAL":
		entry.Severity = ltype.LogSeverity_CRITICAL
	case "FATAL2":
		entry.Severity = ltype.LogSeverity_CRITICAL
	case "FATAL3":
		entry.Severity = ltype.LogSeverity_CRITICAL
	case "FATAL4":
		entry.Severity = ltype.LogSeverity_CRITICAL
	}
	return entry
}
