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
	"net/url"
	"path/filepath"
	"strings"

	conventions "go.opentelemetry.io/collector/translator/conventions/v1.5.0"

	cloudlogging "cloud.google.com/go/logging/apiv2"
	"go.opentelemetry.io/collector/model/pdata"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	loggingpb "google.golang.org/genproto/googleapis/logging/v2"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GoogleLogging struct {
	// GCP project id
	ProjectID string
	// List of labels/attributes to get the log name from
	LogNameFrom []string
	// If no log name can be found, defaults to this
	DefaultLogName string
	// List of labels/attributes to get the service name from
	ServiceNameFrom []string
	// List of labels/attributes to get the service version from
	ServiceVersionFrom []string
	// List of labels/attributes to get the file name from
	FileNameFrom []string
	// Kubernetes container name label
	KubernetesContainerLabelFrom string
	// Kubernetes labels mapping (format is from:to)
	KubernetesLabelsMapping map[string]string

	client *cloudlogging.Client
}

const (
	// default GCP Log name
	DefaultLogName string = "unknown"

	// additional attributes
	AttributeLogName            string = "log.name"
	AttributeFileName           string = "file.name"
	AttributeFileNameUnderscore string = "file_name"

	// GCP resource types
	GoogleResourceTypeGlobal              string = "global"
	GoogleResourceTypeKubernetesContainer string = "k8s_container"

	// GCP resource labels
	GoogleResourceLabelProjectID     string = "project_id"
	GoogleResourceLabelLocation      string = "location"
	GoogleResourceLabelClusterName   string = "cluster_name"
	GoogleResourceLabelNodeName      string = "node_name"
	GoogleResourceLabelNamespaceName string = "namespace_name"
	GoogleResourceLabelPodName       string = "pod_name"
	GoogleResourceLabelContainerName string = "container_name"

	// GCP Error reporting
	GoogleServiceContext        string = "serviceContext"
	GoogleServiceContextService string = "service"
	GoogleServiceContextVersion string = "version"
)

func NewGoogleLogging(ctx context.Context, projectID string) *GoogleLogging {
	client, _ := cloudlogging.NewClient(ctx)
	// inject configuration if needed here
	return &GoogleLogging{
		ProjectID:                    projectID,
		LogNameFrom:                  []string{AttributeLogName, conventions.AttributeServiceName, conventions.AttributeK8SContainerName, conventions.AttributeK8SPodName},
		DefaultLogName:               DefaultLogName,
		ServiceNameFrom:              []string{conventions.AttributeServiceName, AttributeLogName, conventions.AttributeK8SContainerName, conventions.AttributeK8SPodName},
		ServiceVersionFrom:           []string{conventions.AttributeServiceVersion},
		FileNameFrom:                 []string{AttributeFileName, AttributeFileNameUnderscore},
		KubernetesContainerLabelFrom: conventions.AttributeK8SContainerName,
		KubernetesLabelsMapping: map[string]string{
			conventions.AttributeCloudAccountID:        GoogleResourceLabelProjectID,
			conventions.AttributeCloudAvailabilityZone: GoogleResourceLabelLocation,
			conventions.AttributeK8SClusterName:        GoogleResourceLabelClusterName,
			conventions.AttributeK8SNodeName:           GoogleResourceLabelNodeName,
			conventions.AttributeK8SNamespaceName:      GoogleResourceLabelNamespaceName,
			conventions.AttributeK8SPodName:            GoogleResourceLabelPodName,
			conventions.AttributeK8SContainerName:      GoogleResourceLabelContainerName,
		},
		client: client,
	}
}

type GoogleLoggingBatch struct {
	exporter   *GoogleLogging
	logEntries []*loggingpb.LogEntry
}

func (b *GoogleLoggingBatch) Append(rl []*loggingpb.LogEntry) {
	b.logEntries = append(b.logEntries, rl...)
}

func (b *GoogleLoggingBatch) WriteBatches() {
	logMap := make(map[string][]*loggingpb.LogEntry)
	for i := range b.logEntries {
		entry := b.logEntries[i]
		entries, ok := logMap[entry.LogName]
		if !ok {
			entries = []*loggingpb.LogEntry{entry}
		} else {
			entries = append(entries, entry)
		}
		logMap[entry.LogName] = entries
	}
	for logName, entries := range logMap {
		b.writeBatch(logName, entries)
	}
}

func (b *GoogleLoggingBatch) writeBatch(logName string, entries []*loggingpb.LogEntry) {
	request := loggingpb.WriteLogEntriesRequest{
		LogName: logName,
		Entries: entries,
	}

	response, err := b.exporter.client.WriteLogEntries(
		context.TODO(),
		&request,
	)
	if err != nil {
		// TODO log error here
	} else {
		_ = response
	}
}

func (gl *GoogleLogging) NewBatch() *GoogleLoggingBatch {
	return &GoogleLoggingBatch{
		exporter:   gl,
		logEntries: make([]*loggingpb.LogEntry, 0),
	}
}

func (gl *GoogleLogging) ToLogEntries(logs pdata.ResourceLogs) []*loggingpb.LogEntry {
	il := logs.InstrumentationLibraryLogs()
	logEntries := make([]*loggingpb.LogEntry, 0)
	for i := 0; i < il.Len(); i++ {
		l := il.At(i)
		logSlice := l.Logs()
		for j := 0; j < logSlice.Len(); j++ {
			logEntries = append(logEntries, gl.ToLogEntry(logs, l, logSlice.At(j)))
		}
	}
	return logEntries
}

func (gl *GoogleLogging) Shutdown(context.Context) error {
	return gl.client.Close()
}

func (gl *GoogleLogging) ToLogEntry(logs pdata.ResourceLogs, il pdata.InstrumentationLibraryLogs, logRecord pdata.LogRecord) *loggingpb.LogEntry {
	// default resource
	monitoredResource := &monitoredres.MonitoredResource{Type: GoogleResourceTypeGlobal}

	labels := extractLabels(logs, il, logRecord)

	// convert to Kubernetes container resource
	if container, found := labels[gl.KubernetesContainerLabelFrom]; found && container != "" {
		monitoredResource = gl.getKubernetesContainerResource(labels)
	}

	entry := &loggingpb.LogEntry{
		Timestamp: &timestamppb.Timestamp{
			Seconds: logRecord.Timestamp().AsTime().Unix(),
			Nanos:   int32(logRecord.Timestamp().AsTime().Nanosecond()),
		},
		Labels:         labels,
		Resource:       monitoredResource,
		LogName:        gl.getLogName(labels),
		Severity:       getSeverity(logRecord.SeverityText()),
		HttpRequest:    getHTTPRequest(logRecord),
		SourceLocation: getSourceLocation(logRecord),
	}

	// build a service context
	var serviceContext map[string]interface{}
	if serviceName, found := getFirstNotEmptyLabel(labels, gl.ServiceNameFrom); found {
		serviceContext = gl.getServiceContext(serviceName, labels)
	}

	// build the body
	if err := gl.addBodyToLogEntry(entry, logRecord.Body(), serviceContext); err != nil {
		// TODO log error
		_ = err
	}

	// add trace data
	gl.addTraceToLogEntry(entry, logRecord)

	return entry
}

// See https://cloud.google.com/logging/docs/api/v2/resource-list#resource-types
func (gl *GoogleLogging) getKubernetesContainerResource(labels map[string]string) *monitoredres.MonitoredResource {
	k8sLabels := map[string]string{}
	for from, to := range gl.KubernetesLabelsMapping {
		if v, found := labels[from]; found && v != "" {
			k8sLabels[to] = v
		}
	}
	return &monitoredres.MonitoredResource{Type: GoogleResourceTypeKubernetesContainer, Labels: k8sLabels}
}

func (gl *GoogleLogging) getLogName(labels map[string]string) string {
	logName, found := getFirstNotEmptyLabel(labels, gl.LogNameFrom)
	if !found {
		if fileName, found := getFirstNotEmptyLabel(labels, gl.FileNameFrom); found {
			logName = strings.TrimSuffix(fileName, filepath.Ext(fileName))
			if idx := strings.Index(fileName, "-"); idx >= 0 {
				logName = logName[:idx]
			}
		} else {
			logName = gl.DefaultLogName
		}
	}
	return fmt.Sprintf("projects/%s/logs/%s", gl.ProjectID, url.QueryEscape(logName))
}

func (gl *GoogleLogging) addBodyToLogEntry(entry *loggingpb.LogEntry, value pdata.AttributeValue, serviceContext map[string]interface{}) error {
	switch value.Type() {
	case pdata.AttributeValueTypeString:
		// TODO detect JSON payload?
		entry.Payload = &loggingpb.LogEntry_TextPayload{
			TextPayload: value.StringVal(),
		}
	case pdata.AttributeValueTypeMap:
		converted := pdata.AttributeMapToMap(value.MapVal())

		// inject service context for GCP Error reporting
		if converted[GoogleServiceContext] == nil && len(serviceContext) > 0 {
			converted[GoogleServiceContext] = serviceContext
		}

		json, err := structpb.NewStruct(converted)
		if err != nil {
			return err
		}

		entry.Payload = &loggingpb.LogEntry_JsonPayload{
			JsonPayload: json,
		}
	case pdata.AttributeValueTypeInt:
	case pdata.AttributeValueTypeDouble:
	case pdata.AttributeValueTypeBool:
	case pdata.AttributeValueTypeArray:
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

func extractLabels(logs pdata.ResourceLogs, il pdata.InstrumentationLibraryLogs, logRecord pdata.LogRecord) map[string]string {
	labels := map[string]string{}
	logs.Resource().Attributes().Range(func(k string, v pdata.AttributeValue) bool {
		labels[k] = v.StringVal()
		return true
	})
	// message attributes take precedence over resource attributes
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

func getSeverity(severityText string) ltype.LogSeverity {
	severity := ltype.LogSeverity_DEFAULT
	switch severityText = strings.ToUpper(severityText); {
	case strings.HasPrefix(severityText, "TRACE"):
		severity = ltype.LogSeverity_DEFAULT
	case strings.HasPrefix(severityText, "DEBUG"):
		severity = ltype.LogSeverity_DEBUG
	case strings.HasPrefix(severityText, "INFO"):
		severity = ltype.LogSeverity_INFO
	case strings.HasPrefix(severityText, "WARN"):
		severity = ltype.LogSeverity_WARNING
	case strings.HasPrefix(severityText, "ERROR"):
		severity = ltype.LogSeverity_ERROR
	case strings.HasPrefix(severityText, "FATAL"):
		severity = ltype.LogSeverity_CRITICAL
	}
	return severity
}

// create service context for GCP Error reporting
// ref: https://cloud.google.com/error-reporting/docs/formatting-error-messages
func (gl *GoogleLogging) getServiceContext(serviceName string, labels map[string]string) map[string]interface{} {
	serviceContext := map[string]interface{}{
		GoogleServiceContextService: serviceName,
	}
	if version, found := getFirstNotEmptyLabel(labels, gl.ServiceVersionFrom); found {
		serviceContext[GoogleServiceContextVersion] = version
	}
	return serviceContext
}

// ref. https://pkg.go.dev/google.golang.org/genproto@v0.0.0-20211019152133-63b7e35f4404/googleapis/logging/type#HttpRequest
func getHTTPRequest(logRecord pdata.LogRecord) *ltype.HttpRequest {
	// To be implemented
	return nil
}

// ref. https://pkg.go.dev/google.golang.org/genproto/googleapis/logging/v2?utm_source=gopls#LogEntrySourceLocation
func getSourceLocation(logRecord pdata.LogRecord) *loggingpb.LogEntrySourceLocation {
	// To be implemented
	return nil
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
