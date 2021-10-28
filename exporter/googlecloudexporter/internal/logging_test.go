package internal

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/model/pdata"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	loggingpb "google.golang.org/genproto/googleapis/logging/v2"
)

func TestGoogleLogging_ToLogEntry(t *testing.T) {
	type args struct {
		logs      pdata.ResourceLogs
		il        pdata.InstrumentationLibraryLogs
		logRecord pdata.LogRecord
	}
	tests := []struct {
		name string
		args args
		want *loggingpb.LogEntry
	}{
		{
			name: "Empty",
			args: args{
				logs:      newResourceLogs(map[string]pdata.AttributeValue{}),
				il:        newInstrumentationLibraryLogs("", ""),
				logRecord: newLogRecord(map[string]pdata.AttributeValue{}),
			},
			want: &loggingpb.LogEntry{
				Timestamp: timestamppb.New(time.UnixMilli(0)),
				Resource:  &monitoredres.MonitoredResource{Type: "global"},
				LogName:   "projects/my-project-id/logs/unknown",
				Labels:    map[string]string{},
			},
		},
		{
			name: "VM-like logs",
			args: args{
				logs: newResourceLogs(map[string]pdata.AttributeValue{
					"service.namespace":   pdata.NewAttributeValueString("cdic"),
					"service.name":        pdata.NewAttributeValueString("dgc"),
					"service.instance.id": pdata.NewAttributeValueString("xxxx-yyyy-zzzz"),
					"service.version":     pdata.NewAttributeValueString("yyyy.mm.pp-bb"),
				}),
				il: newInstrumentationLibraryLogs("my-lib", "999.999.999"),
				logRecord: newLogRecordWithBody(map[string]pdata.AttributeValue{
					"file.name": pdata.NewAttributeValueString("dgc.jsonl"),
					"log.name":  pdata.NewAttributeValueString("dgc"),
				}, newAttributeMap(map[string]pdata.AttributeValue{
					"level":   pdata.NewAttributeValueString("INFO"),
					"message": pdata.NewAttributeValueString("Hello world"),
				})),
			},
			want: &loggingpb.LogEntry{
				Timestamp: timestamppb.New(time.UnixMilli(0)),
				Resource:  &monitoredres.MonitoredResource{Type: "global"},
				LogName:   "projects/my-project-id/logs/dgc",
				Labels: map[string]string{
					"service.namespace":                      "cdic",
					"service.name":                           "dgc",
					"service.instance.id":                    "xxxx-yyyy-zzzz",
					"service.version":                        "yyyy.mm.pp-bb",
					"file.name":                              "dgc.jsonl",
					"log.name":                               "dgc",
					"opentelemetry.org/instrumentation/name": "my-lib",
					"opentelemetry.org/instrumentation/version": "999.999.999",
				},
				Payload: &loggingpb.LogEntry_JsonPayload{
					JsonPayload: newBodyStruct(map[string]interface{}{
						"level":   "INFO",
						"message": "Hello world",
						"serviceContext": map[string]interface{}{
							"service": "dgc",
							"version": "yyyy.mm.pp-bb",
						},
					}),
				},
			},
		},
		{
			name: "VM-like logs (without OTel semantic fields)",
			args: args{
				logs: newResourceLogs(map[string]pdata.AttributeValue{}),
				il:   newInstrumentationLibraryLogs("my-lib", "999.999.999"),
				logRecord: newLogRecordWithBody(map[string]pdata.AttributeValue{
					"file.name": pdata.NewAttributeValueString("dgc.jsonl"),
				}, newAttributeMap(map[string]pdata.AttributeValue{
					"level":   pdata.NewAttributeValueString("INFO"),
					"message": pdata.NewAttributeValueString("Hello world"),
				})),
			},
			want: &loggingpb.LogEntry{
				Timestamp: timestamppb.New(time.UnixMilli(0)),
				Resource:  &monitoredres.MonitoredResource{Type: "global"},
				LogName:   "projects/my-project-id/logs/dgc",
				Labels: map[string]string{
					"file.name":                                 "dgc.jsonl",
					"opentelemetry.org/instrumentation/name":    "my-lib",
					"opentelemetry.org/instrumentation/version": "999.999.999",
				},
				Payload: &loggingpb.LogEntry_JsonPayload{
					JsonPayload: newBodyStruct(map[string]interface{}{
						"level":   "INFO",
						"message": "Hello world",
					}),
				},
			},
		},
		{
			name: "K8s-like logs",
			args: args{
				logs: newResourceLogs(map[string]pdata.AttributeValue{
					"cloud.account.id":        pdata.NewAttributeValueString("my-account-id"),
					"cloud.availability_zone": pdata.NewAttributeValueString("eu-somewhere-1"),
					"k8s.cluster.name":        pdata.NewAttributeValueString("cluster-name"),
					"k8s.node.name":           pdata.NewAttributeValueString("node-name"),
					"k8s.namespace.name":      pdata.NewAttributeValueString("cdic-xxxx-yyyy-zzzz"),
					"k8s.pod.name":            pdata.NewAttributeValueString("dgc-core-xxxxx"),
					"k8s.container.name":      pdata.NewAttributeValueString("dgc-core"),
				}),
				il: newInstrumentationLibraryLogs("my-lib", "999.999.999"),
				logRecord: newLogRecordWithBody(map[string]pdata.AttributeValue{
					"file.name": pdata.NewAttributeValueString("dgc-core-xxxxx.log"),
				}, newAttributeMap(map[string]pdata.AttributeValue{
					"level":   pdata.NewAttributeValueString("INFO"),
					"message": pdata.NewAttributeValueString("Hello world"),
				})),
			},
			want: &loggingpb.LogEntry{
				Timestamp: timestamppb.New(time.UnixMilli(0)),
				Resource: &monitoredres.MonitoredResource{
					Type: "k8s_container",
					Labels: map[string]string{
						"project_id":     "my-account-id",
						"location":       "eu-somewhere-1",
						"cluster_name":   "cluster-name",
						"node_name":      "node-name",
						"namespace_name": "cdic-xxxx-yyyy-zzzz",
						"pod_name":       "dgc-core-xxxxx",
						"container_name": "dgc-core",
					},
				},
				LogName: "projects/my-project-id/logs/dgc-core",
				Labels: map[string]string{
					"cloud.account.id":                          "my-account-id",
					"cloud.availability_zone":                   "eu-somewhere-1",
					"k8s.cluster.name":                          "cluster-name",
					"k8s.node.name":                             "node-name",
					"k8s.namespace.name":                        "cdic-xxxx-yyyy-zzzz",
					"k8s.pod.name":                              "dgc-core-xxxxx",
					"k8s.container.name":                        "dgc-core",
					"file.name":                                 "dgc-core-xxxxx.log",
					"opentelemetry.org/instrumentation/name":    "my-lib",
					"opentelemetry.org/instrumentation/version": "999.999.999",
				},
				Payload: &loggingpb.LogEntry_JsonPayload{
					JsonPayload: newBodyStruct(map[string]interface{}{
						"level":          "INFO",
						"message":        "Hello world",
						"serviceContext": map[string]interface{}{"service": "dgc-core"},
					}),
				},
			},
		},
	}
	gl := NewGoogleLogging(context.Background(), "my-project-id")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := gl.ToLogEntry(tt.args.logs, tt.args.il, tt.args.logRecord)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGoogleLogging_addBodyToLogEntry(t *testing.T) {
	type args struct {
		entry          *loggingpb.LogEntry
		body           pdata.AttributeValue
		serviceContext map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want loggingpb.LogEntry
	}{
		{
			name: "Empty",
			args: args{
				entry:          &loggingpb.LogEntry{},
				body:           pdata.NewAttributeValueEmpty(),
				serviceContext: nil,
			},
			want: loggingpb.LogEntry{},
		},
		{
			name: "Text payload",
			args: args{
				entry:          &loggingpb.LogEntry{},
				body:           pdata.NewAttributeValueString("some body here"),
				serviceContext: map[string]interface{}{"not": "supported"},
			},
			want: loggingpb.LogEntry{
				Payload: &loggingpb.LogEntry_TextPayload{TextPayload: "some body here"},
			},
		},
		{
			name: "JSON payload",
			args: args{
				entry: &loggingpb.LogEntry{},
				body: newAttributeMap(map[string]pdata.AttributeValue{
					"level":   pdata.NewAttributeValueString("severely-burned"),
					"message": pdata.NewAttributeValueString("Hello world"),
				}),
				serviceContext: map[string]interface{}{"some": "stuff"},
			},
			want: loggingpb.LogEntry{
				Payload: &loggingpb.LogEntry_JsonPayload{
					JsonPayload: newBodyStruct(map[string]interface{}{
						"level":          "severely-burned",
						"message":        "Hello world",
						"serviceContext": map[string]interface{}{"some": "stuff"},
					}),
				},
			},
		},
		{
			name: "JSON payload, no service context",
			args: args{
				entry: &loggingpb.LogEntry{},
				body: newAttributeMap(map[string]pdata.AttributeValue{
					"level":   pdata.NewAttributeValueString("informal"),
					"message": pdata.NewAttributeValueString("Hello universe"),
				}),
				serviceContext: nil,
			},
			want: loggingpb.LogEntry{
				Payload: &loggingpb.LogEntry_JsonPayload{
					JsonPayload: newBodyStruct(map[string]interface{}{
						"level":   "informal",
						"message": "Hello universe",
					}),
				},
			},
		},
	}
	gl := NewGoogleLogging(context.Background(), "my-project-id")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := gl.addBodyToLogEntry(tt.args.entry, tt.args.body, tt.args.serviceContext)
			assert.NoError(t, err)
			got := *tt.args.entry
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGoogleLogging_addTraceToLogEntry(t *testing.T) {
	type args struct {
		entry     *loggingpb.LogEntry
		logRecord pdata.LogRecord
	}

	traceID := [16]byte{0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78}
	spanID := [8]byte{0x12, 0x23, 0xAD, 0x12, 0x23, 0xAD, 0x12, 0x23}

	tests := []struct {
		name string
		args args
		want loggingpb.LogEntry
	}{
		{
			name: "Empty",
			args: args{
				entry:     &loggingpb.LogEntry{},
				logRecord: newLogRecord(map[string]pdata.AttributeValue{}),
			},
			want: loggingpb.LogEntry{},
		},
		{
			name: "Not sampled",
			args: args{
				entry:     &loggingpb.LogEntry{},
				logRecord: newLogRecordWithTraces(traceID, spanID, 0x00),
			},
			want: loggingpb.LogEntry{
				Trace:        "projects/my-project-id/traces/12345678123456781234567812345678",
				SpanId:       "1223ad1223ad1223",
				TraceSampled: false,
			},
		},
		{
			name: "Full",
			args: args{
				entry:     &loggingpb.LogEntry{},
				logRecord: newLogRecordWithTraces(traceID, spanID, 0x01),
			},
			want: loggingpb.LogEntry{
				Trace:        "projects/my-project-id/traces/12345678123456781234567812345678",
				SpanId:       "1223ad1223ad1223",
				TraceSampled: true,
			},
		},
	}
	gl := NewGoogleLogging(context.Background(), "my-project-id")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gl.addTraceToLogEntry(tt.args.entry, tt.args.logRecord)
			got := *tt.args.entry
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGoogleLogging_getKubernetesContainerResource(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   *monitoredres.MonitoredResource
	}{
		{
			name:   "Empty",
			labels: nil,
			want:   &monitoredres.MonitoredResource{Type: "k8s_container", Labels: map[string]string{}},
		},
		{
			name:   "Some labels",
			labels: map[string]string{"some": "labels"},
			want:   &monitoredres.MonitoredResource{Type: "k8s_container", Labels: map[string]string{}},
		},
		{
			name: "All Kubernetes labels",
			labels: map[string]string{
				"cloud.account.id":        "some-id",
				"cloud.availability_zone": "eu-somewhere-1a",
				"k8s.cluster.name":        "cluster-name",
				"k8s.node.name":           "node-name",
				"k8s.namespace.name":      "namespace-name",
				"k8s.pod.name":            "pod-name",
				"k8s.container.name":      "container-name",
			},
			want: &monitoredres.MonitoredResource{
				Type: "k8s_container",
				Labels: map[string]string{
					"project_id":     "some-id",
					"location":       "eu-somewhere-1a",
					"cluster_name":   "cluster-name",
					"node_name":      "node-name",
					"namespace_name": "namespace-name",
					"pod_name":       "pod-name",
					"container_name": "container-name",
				},
			},
		},
		{
			name: "Partial Kubernetes labels",
			labels: map[string]string{
				"k8s.container.name": "container-name",
			},
			want: &monitoredres.MonitoredResource{
				Type: "k8s_container",
				Labels: map[string]string{
					"container_name": "container-name",
				},
			},
		},
	}
	gl := NewGoogleLogging(context.Background(), "my-project-id")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := gl.getKubernetesContainerResource(tt.labels)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGoogleLogging_getLogName(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{
			name:   "Empty",
			labels: nil,
			want:   "projects/my-project-id/logs/unknown",
		},
		{
			name:   "Encoded log name",
			labels: map[string]string{"log.name": "hello/world"},
			want:   "projects/my-project-id/logs/hello%2Fworld",
		},
		{
			name:   "Fallback on file name",
			labels: map[string]string{"file.name": "hello"},
			want:   "projects/my-project-id/logs/hello",
		},
		{
			name:   "Extract log name from file name with date",
			labels: map[string]string{"file.name": "hello-yyyy-mm-dd.log"},
			want:   "projects/my-project-id/logs/hello",
		},
		{
			name:   "Extract log name from file name",
			labels: map[string]string{"file.name": "hello.log"},
			want:   "projects/my-project-id/logs/hello",
		},
	}
	gl := NewGoogleLogging(context.Background(), "my-project-id")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := gl.getLogName(tt.labels)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_extractLabels(t *testing.T) {
	type args struct {
		logs      pdata.ResourceLogs
		il        pdata.InstrumentationLibraryLogs
		logRecord pdata.LogRecord
	}
	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		{
			name: "Empty",
			args: args{
				logs:      newResourceLogs(map[string]pdata.AttributeValue{}),
				il:        newInstrumentationLibraryLogs("", ""),
				logRecord: newLogRecord(map[string]pdata.AttributeValue{}),
			},
			want: map[string]string{},
		},
		{
			name: "Instrumentation library",
			args: args{
				logs:      newResourceLogs(map[string]pdata.AttributeValue{}),
				il:        newInstrumentationLibraryLogs("my-name", "0.0.1-alpha5"),
				logRecord: newLogRecord(map[string]pdata.AttributeValue{}),
			},
			want: map[string]string{
				"opentelemetry.org/instrumentation/name":    "my-name",
				"opentelemetry.org/instrumentation/version": "0.0.1-alpha5",
			},
		},
		{
			name: "Full example",
			args: args{
				logs: newResourceLogs(map[string]pdata.AttributeValue{
					"resource-logs": pdata.NewAttributeValueString("other value here"),
					"some-key":      pdata.NewAttributeValueString("resource logs"),
				}),
				il: newInstrumentationLibraryLogs("hello", "0.0.2-beta1"),
				logRecord: newLogRecord(map[string]pdata.AttributeValue{
					"log-record": pdata.NewAttributeValueString("value here"),
					"some-key":   pdata.NewAttributeValueString("log record"),
				}),
			},
			want: map[string]string{
				"resource-logs":                          "other value here",
				"log-record":                             "value here",
				"some-key":                               "log record",
				"opentelemetry.org/instrumentation/name": "hello",
				"opentelemetry.org/instrumentation/version": "0.0.2-beta1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractLabels(tt.args.logs, tt.args.il, tt.args.logRecord)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGoogleLogging_getServiceContext(t *testing.T) {
	type args struct {
		serviceName string
		labels      map[string]string
	}
	tests := []struct {
		name string
		args args
		want map[string]interface{}
	}{
		{
			name: "Empty",
			args: args{},
			want: map[string]interface{}{"service": ""},
		},
		{
			name: "No version",
			args: args{
				serviceName: "my-service",
				labels:      map[string]string{},
			},
			want: map[string]interface{}{"service": "my-service"},
		},
		{
			name: "Full",
			args: args{
				serviceName: "service-b",
				labels:      map[string]string{"service.version": "0.0.1-alpha3"},
			},
			want: map[string]interface{}{
				"service": "service-b",
				"version": "0.0.1-alpha3",
			},
		},
	}
	gl := NewGoogleLogging(context.Background(), "my-project-id")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := gl.getServiceContext(tt.args.serviceName, tt.args.labels)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_getFirstNotEmptyLabel(t *testing.T) {
	type args struct {
		labels map[string]string
		keys   []string
	}
	type result struct {
		value string
		found bool
	}
	tests := []struct {
		name string
		args args
		want result
	}{
		{
			name: "Empty labels",
			args: args{
				labels: nil,
				keys:   nil,
			},
			want: result{value: "", found: false},
		},
		{
			name: "Not found",
			args: args{
				labels: map[string]string{
					"stuff": "here",
					"hello": "world",
				},
				keys: []string{"nope", "not.here"},
			},
			want: result{value: "", found: false},
		},
		{
			name: "One key",
			args: args{
				labels: map[string]string{
					"stuff": "here",
					"hello": "world",
					"extra": "!",
				},
				keys: []string{"stuff"},
			},
			want: result{value: "here", found: true},
		},
		{
			name: "Multiple keys",
			args: args{
				labels: map[string]string{
					"stuff": "here",
					"hello": "world",
					"extra": "!",
				},
				keys: []string{"hello", "stuff", "extra"},
			},
			want: result{value: "world", found: true},
		},
		{
			name: "Multiple keys with empty value",
			args: args{
				labels: map[string]string{
					"hello": "",
					"extra": "!",
				},
				keys: []string{"hello", "stuff", "extra"},
			},
			want: result{value: "!", found: true},
		},
		{
			name: "Multiple keys with only empty values",
			args: args{
				labels: map[string]string{
					"stuff": "",
					"hello": "",
					"extra": "",
				},
				keys: []string{"hello", "stuff", "extra"},
			},
			want: result{value: "", found: false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, found := getFirstNotEmptyLabel(tt.args.labels, tt.args.keys)
			got := result{value: value, found: found}
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_getSeverity(t *testing.T) {
	tests := []struct {
		name string
		args string
		want ltype.LogSeverity
	}{
		{
			name: "Empty",
			args: "",
			want: ltype.LogSeverity_DEFAULT,
		}, {
			name: "Non-matched",
			args: "whatever",
			want: ltype.LogSeverity_DEFAULT,
		}, {
			name: "Trace",
			args: "trace",
			want: ltype.LogSeverity_DEFAULT,
		}, {
			name: "Trace (by prefix)",
			args: "TRACE5",
			want: ltype.LogSeverity_DEFAULT,
		}, {
			name: "Debug",
			args: "debug",
			want: ltype.LogSeverity_DEBUG,
		}, {
			name: "Info",
			args: "info",
			want: ltype.LogSeverity_INFO,
		}, {
			name: "Warning",
			args: "warn",
			want: ltype.LogSeverity_WARNING,
		}, {
			name: "Error",
			args: "error",
			want: ltype.LogSeverity_ERROR,
		}, {
			name: "Critical",
			args: "fatal",
			want: ltype.LogSeverity_CRITICAL,
		}, {
			name: "Crazy stuff",
			args: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			want: ltype.LogSeverity_DEFAULT,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getSeverity(tt.args)
			assert.Equal(t, tt.want, got)
		})
	}
}

// helpers

func newResourceLogs(attr map[string]pdata.AttributeValue) pdata.ResourceLogs {
	resourceLogs := pdata.NewResourceLogs()
	resourceLogs.Resource().Attributes().InitFromMap(attr)
	return resourceLogs
}

func newInstrumentationLibraryLogs(name, version string) pdata.InstrumentationLibraryLogs {
	il := pdata.NewInstrumentationLibraryLogs()
	il.InstrumentationLibrary().SetName(name)
	il.InstrumentationLibrary().SetVersion(version)
	return il
}

func newLogRecord(attr map[string]pdata.AttributeValue) pdata.LogRecord {
	logRecord := pdata.NewLogRecord()
	logRecord.SetTimestamp(pdata.NewTimestampFromTime(time.UnixMilli(0)))
	logRecord.Attributes().InitFromMap(attr)
	return logRecord
}

func newLogRecordWithBody(attr map[string]pdata.AttributeValue, body pdata.AttributeValue) pdata.LogRecord {
	logRecord := pdata.NewLogRecord()
	logRecord.SetTimestamp(pdata.NewTimestampFromTime(time.UnixMilli(0)))
	logRecord.Attributes().InitFromMap(attr)
	body.CopyTo(logRecord.Body())
	return logRecord
}

func newLogRecordWithTraces(traceID [16]byte, spanID [8]byte, flags uint32) pdata.LogRecord {
	logRecord := pdata.NewLogRecord()
	logRecord.SetTraceID(pdata.NewTraceID(traceID))
	logRecord.SetSpanID(pdata.NewSpanID(spanID))
	logRecord.SetFlags(flags)
	return logRecord
}

func newBodyStruct(input map[string]interface{}) *structpb.Struct {
	s, _ := structpb.NewStruct(input)
	return s
}

func newAttributeMap(input map[string]pdata.AttributeValue) pdata.AttributeValue {
	m := pdata.NewAttributeValueMap()
	m.MapVal().InitFromMap(input)
	return m
}
