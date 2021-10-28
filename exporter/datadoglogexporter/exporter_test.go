package datadoglogexporter

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadoglogexporter/internal/client"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/model/pdata"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap/zaptest"
)

func TestDatadogExporterBatch_WriteBatch_NoMessage(t *testing.T) {
	exp, deferFunc := newTestDatadogExporterWithTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Fail(t, "no API call should have happened")
	}))
	defer deferFunc()

	b := exp.newBatch()
	assert.Equal(t, 0, b.buffer.Len())
	require.Equal(t, 0, b.count)

	err := b.WriteBatch()
	assert.NoError(t, err)
	assert.Equal(t, 0, b.buffer.Len())
	require.Equal(t, 0, b.count)
}

func TestDatadogExporterBatch_WriteBatch_OneMessage(t *testing.T) {
	exp, deferFunc := newTestDatadogExporterWithTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, client.ContentTypeApplicationJSON, req.Header.Get(client.HeaderContentType))
		assert.Equal(t, "otelcol/latest", req.Header.Get(client.HeaderUserAgent))
		assert.Equal(t, "api-key", req.Header.Get(client.HeaderDatadogAPIKey))

		data, err := ioutil.ReadAll(req.Body)
		defer req.Body.Close()
		assert.NoError(t, err)
		assert.JSONEq(t, `[{"service":"service","message":{"a":"b"}}]`, string(data))

		w.WriteHeader(http.StatusOK)
	}))
	defer deferFunc()

	b := exp.newBatch()

	b.Append(&datadogMessage{
		Service: "service",
		Message: map[string]interface{}{"a": "b"},
	})
	assert.Greater(t, b.buffer.Len(), 0)
	require.Equal(t, 1, b.count)

	err := b.WriteBatch()
	assert.NoError(t, err)
	assert.Equal(t, 0, b.buffer.Len())
	require.Equal(t, 0, b.count)
}

func TestDatadogExporterBatch_WriteBatch_MultipleMessages(t *testing.T) {
	exp, deferFunc := newTestDatadogExporterWithTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, client.ContentTypeApplicationJSON, req.Header.Get(client.HeaderContentType))
		assert.Equal(t, "otelcol/latest", req.Header.Get(client.HeaderUserAgent))
		assert.Equal(t, "api-key", req.Header.Get(client.HeaderDatadogAPIKey))

		data, err := ioutil.ReadAll(req.Body)
		defer req.Body.Close()
		assert.NoError(t, err)
		assert.JSONEq(t, `[
			{"service":"service","message":{"a":"b"}},
			{"service":"service2","ddsource":"source","ddtags":"some:tag,other:tag,self-tag","hostname":"my-host","message":{"a":42}}
		]`, string(data))

		w.WriteHeader(http.StatusOK)
	}))
	defer deferFunc()

	b := exp.newBatch()

	b.Append(&datadogMessage{
		Service: "service",
		Message: map[string]interface{}{"a": "b"},
	})
	assert.Greater(t, b.buffer.Len(), 0)
	require.Equal(t, 1, b.count)

	b.Append(&datadogMessage{
		Service:  "service2",
		Source:   "source",
		Tags:     "some:tag,other:tag,self-tag",
		Hostname: "my-host",
		Message:  map[string]interface{}{"a": 42},
	})
	assert.Greater(t, b.buffer.Len(), 0)
	require.Equal(t, 2, b.count)

	err := b.WriteBatch()
	assert.NoError(t, err)
	assert.Equal(t, 0, b.buffer.Len())
	require.Equal(t, 0, b.count)
}

func TestDatadogExporterBatch_WriteBatch_MaxBatchSize(t *testing.T) {
	batchesReceived := 0
	exp, deferFunc := newTestDatadogExporterWithTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		batchesReceived++
		w.WriteHeader(http.StatusOK)
	}))
	defer deferFunc()

	b := exp.newBatch()
	b.maxBatchSize = 50

	msg := &datadogMessage{Service: "service", Message: map[string]interface{}{"a": "b"}}
	for i := 0; i < 5; i++ {
		b.Append(msg)
		require.Less(t, b.buffer.Len(), 50)
		require.Equal(t, i, batchesReceived)
	}

	err := b.WriteBatch()
	assert.NoError(t, err)
	require.Equal(t, 0, b.buffer.Len())
	require.Equal(t, 5, batchesReceived)
}

func TestDatadogExporterBatch_WriteBatch_MaxBatchMessages(t *testing.T) {
	batchesReceived := 0
	exp, deferFunc := newTestDatadogExporterWithTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		batchesReceived++
		w.WriteHeader(http.StatusOK)
	}))
	defer deferFunc()

	b := exp.newBatch()
	b.maxBatchMessages = 1

	msg := &datadogMessage{Service: "service", Message: map[string]interface{}{"a": "b"}}
	for i := 0; i < 5; i++ {
		b.Append(msg)
		require.Equal(t, 1, b.count)
		require.Equal(t, i, batchesReceived)
	}

	err := b.WriteBatch()
	assert.NoError(t, err)
	assert.Equal(t, 0, b.count)
	require.Equal(t, 5, batchesReceived)
}

func Test_datadogExporter_ConsumeLogs(t *testing.T) {
	// TODO
}

func Test_datadogExporter_completeMessage(t *testing.T) {
	type args struct {
		messageMap map[string]interface{}
		record     pdata.LogRecord
	}

	traceID := [16]byte{0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78}
	spanID := [8]byte{0x12, 0x23, 0xAD, 0x12, 0x23, 0xAD, 0x12, 0x23}

	tests := []struct {
		name string
		args args
		want map[string]interface{}
	}{
		{
			name: "Empty",
			args: args{
				messageMap: map[string]interface{}{},
				record:     pdata.NewLogRecord(),
			},
			want: map[string]interface{}{
				"timestamp": "1970-01-01T00:00:00.000Z",
			},
		},
		{
			name: "With severity",
			args: args{
				messageMap: map[string]interface{}{},
				record:     newLogRecordWithSeverity("INFO"),
			},
			want: map[string]interface{}{
				"timestamp": "1970-01-01T00:00:00.000Z",
				"status":    "INFO",
			},
		},
		{
			name: "With trace",
			args: args{
				messageMap: map[string]interface{}{},
				record:     newLogRecordWithTraces(traceID, spanID),
			},
			want: map[string]interface{}{
				"timestamp": "1970-01-01T00:00:00.000Z",
				"trace_id":  "12345678123456781234567812345678",
				"span_id":   "1223ad1223ad1223",
			},
		},
	}
	exp := newTestDatadogExporter(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exp.completeMessage(tt.args.messageMap, tt.args.record)
			got := tt.args.messageMap
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_datadogExporter_getHostName(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{
			name:   "Empty",
			labels: nil,
			want:   "",
		},
		{
			name:   "Host name",
			labels: map[string]string{"host.name": "my-host"},
			want:   "my-host",
		},
	}
	exp := newTestDatadogExporter(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exp.getHostname(tt.labels)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_datadogExporter_getMessageMapFromText(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    map[string]interface{}
	}{
		{
			name:    "Empty",
			message: "",
			want:    map[string]interface{}{"message": ""},
		},
		{
			name: "JSON serialized",
			message: `{
				"some": "stuff",
				"other": "things",
				"message": "some interesting logs"
			}`,
			want: map[string]interface{}{
				"some":    "stuff",
				"other":   "things",
				"message": "some interesting logs",
			},
		},
		{
			name:    "Invalid JSON",
			message: `{message": some interesting logs}`,
			want: map[string]interface{}{
				"message": `{message": some interesting logs}`,
			},
		},
	}
	exp := newTestDatadogExporter(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exp.getMessageMapFromText(tt.message)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_datadogExporter_getService(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{
			name:   "Empty",
			labels: nil,
			want:   "",
		},
		{
			name:   "Service name",
			labels: map[string]string{"service.name": "service"},
			want:   "service",
		},
		{
			name:   "Log name",
			labels: map[string]string{"log.name": "log"},
			want:   "log",
		},
		{
			name:   "Container name",
			labels: map[string]string{"k8s.container.name": "container"},
			want:   "container",
		},
		{
			name:   "Pod name",
			labels: map[string]string{"k8s.pod.name": "pod"},
			want:   "pod",
		},
	}
	exp := newTestDatadogExporter(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exp.getService(tt.labels)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_datadogExporter_getSource(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{
			name:   "Empty",
			labels: nil,
			want:   "",
		},
		{
			name:   "Service type",
			labels: map[string]string{"service.type": "type"},
			want:   "type",
		},
		{
			name:   "Service name",
			labels: map[string]string{"service.name": "service"},
			want:   "service",
		},
		{
			name:   "Log name",
			labels: map[string]string{"log.name": "log"},
			want:   "log",
		},
	}
	exp := newTestDatadogExporter(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exp.getSource(tt.labels)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_datadogExporter_getTags(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   []string
	}{
		{
			name:   "Empty",
			labels: nil,
			want:   []string{""},
		},
		{
			name:   "No value",
			labels: map[string]string{"a-tag": ""},
			want:   []string{"a-tag"},
		},
		{
			name:   "Value",
			labels: map[string]string{"a-tag": "value"},
			want:   []string{"a-tag:value"},
		},
		{
			name: "Full",
			labels: map[string]string{
				"a-tag":    "value",
				"no-value": "",
				"tag2":     "value",
			},
			want: []string{"a-tag:value", "no-value", "tag2:value"},
		},
	}
	exp := newTestDatadogExporter(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exp.getTags(tt.labels)
			assert.ElementsMatch(t, tt.want, strings.Split(got, ","))
		})
	}
}

func Test_datadogExporter_toMessage(t *testing.T) {
	type args struct {
		resource        pdata.ResourceLogs
		instrumentation pdata.InstrumentationLibraryLogs
		log             pdata.LogRecord
	}
	type want struct {
		noValue  bool
		source   string
		tags     []string
		hostname string
		message  map[string]interface{}
		service  string
	}

	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "Empty",
			args: args{
				resource:        pdata.NewResourceLogs(),
				instrumentation: pdata.NewInstrumentationLibraryLogs(),
				log:             pdata.NewLogRecord(),
			},
			want: want{noValue: true},
		},
		{
			name: "Raw string type body",
			args: args{
				resource: newResourceLogs(map[string]pdata.AttributeValue{
					"service.name": pdata.NewAttributeValueString("my-service"),
					"service.type": pdata.NewAttributeValueString("go"),
					"host.name":    pdata.NewAttributeValueString("my-host"),
				}),
				instrumentation: newInstrumentationLibraryLogs("lib", "x.y.z"),
				log:             newLogRecordWithBody(map[string]pdata.AttributeValue{}, pdata.NewAttributeValueString("body here")),
			},
			want: want{
				service:  "my-service",
				source:   "go",
				hostname: "my-host",
				message: map[string]interface{}{
					"timestamp": "1970-01-01T00:00:00.000Z",
					"message":   "body here",
				},
				tags: []string{
					"service.name:my-service",
					"service.type:go",
					"host.name:my-host",
					"opentelemetry.org/instrumentation/name:lib",
					"opentelemetry.org/instrumentation/version:x.y.z",
				},
			},
		},
		{
			name: "JSON string type body",
			args: args{
				resource:        newResourceLogs(map[string]pdata.AttributeValue{}),
				instrumentation: pdata.NewInstrumentationLibraryLogs(),
				log: newLogRecordWithBody(map[string]pdata.AttributeValue{}, pdata.NewAttributeValueString(`
					{
						"message": "in a json body",
						"some-other-data": "is here"
					}
				`)),
			},
			want: want{
				message: map[string]interface{}{
					"timestamp":       "1970-01-01T00:00:00.000Z",
					"message":         "in a json body",
					"some-other-data": "is here",
				},
			},
		},
		{
			name: "Structured type body",
			args: args{
				resource:        newResourceLogs(map[string]pdata.AttributeValue{}),
				instrumentation: newInstrumentationLibraryLogs("", ""),
				log: newLogRecordWithBody(map[string]pdata.AttributeValue{
					"log.name": pdata.NewAttributeValueString("dgc"),
				}, newAttributeMap(map[string]pdata.AttributeValue{
					"some.stuff": pdata.NewAttributeValueString("here"),
					"message":    pdata.NewAttributeValueString("a message"),
				})),
			},
			want: want{
				service: "dgc",
				source:  "dgc",
				message: map[string]interface{}{
					"timestamp":  "1970-01-01T00:00:00.000Z",
					"message":    "a message",
					"some.stuff": "here",
				},
				tags: []string{"log.name:dgc"},
			},
		},
	}
	exp := newTestDatadogExporter(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exp.toMessage(tt.args.resource, tt.args.instrumentation, tt.args.log)
			if tt.want.noValue {
				assert.Nilf(t, got, "nil message should have been returned")
			} else {
				assert.Equalf(t, tt.want.service, got.Service, "service mismatched")
				assert.Equalf(t, tt.want.source, got.Source, "source mismatched")
				assert.Equalf(t, tt.want.hostname, got.Hostname, "hostname mismatched")
				assert.Equalf(t, tt.want.message, got.Message, "message mismatched")
				if tt.want.tags == nil || len(tt.want.tags) == 0 {
					assert.Equalf(t, "", got.Tags, "tags should be empty")
				} else {
					assert.ElementsMatchf(t, tt.want.tags, strings.Split(got.Tags, ","), "tags mismatched")
				}
			}
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
				logRecord: newLogRecordWithAttributes(map[string]pdata.AttributeValue{}),
			},
			want: map[string]string{},
		},
		{
			name: "Instrumentation library",
			args: args{
				logs:      newResourceLogs(map[string]pdata.AttributeValue{}),
				il:        newInstrumentationLibraryLogs("my-name", "0.0.1-alpha5"),
				logRecord: newLogRecordWithAttributes(map[string]pdata.AttributeValue{}),
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
				logRecord: newLogRecordWithAttributes(map[string]pdata.AttributeValue{
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

// helpers

func newTestDatadogExporter(t *testing.T) *datadogExporter {
	exp := &datadogExporter{
		logger:    zaptest.NewLogger(t),
		buildInfo: component.NewDefaultBuildInfo(),
		config:    &Config{},
	}
	_ = exp.Start(context.Background(), componenttest.NewNopHost())
	return exp
}

func newTestDatadogExporterWithTestServer(t *testing.T, handler http.Handler) (*datadogExporter, func()) {
	server := httptest.NewServer(handler)
	exp := &datadogExporter{
		logger:    zaptest.NewLogger(t),
		buildInfo: component.NewDefaultBuildInfo(),
		config:    &Config{URL: server.URL, APIKey: "api-key"},
	}
	_ = exp.Start(context.Background(), componenttest.NewNopHost())
	return exp, func() {
		exp.Shutdown(context.Background())
		server.Close()
	}
}

func newLogRecordWithAttributes(attr map[string]pdata.AttributeValue) pdata.LogRecord {
	logRecord := pdata.NewLogRecord()
	pdata.NewAttributeMapFromMap(attr).CopyTo(logRecord.Attributes())
	return logRecord
}

func newLogRecordWithSeverity(severity string) pdata.LogRecord {
	record := pdata.NewLogRecord()
	record.SetSeverityText(severity)
	return record
}

func newLogRecordWithTraces(traceID [16]byte, spanID [8]byte) pdata.LogRecord {
	logRecord := pdata.NewLogRecord()
	logRecord.SetTraceID(pdata.NewTraceID(traceID))
	logRecord.SetSpanID(pdata.NewSpanID(spanID))
	return logRecord
}

func newLogRecordWithBody(attr map[string]pdata.AttributeValue, body pdata.AttributeValue) pdata.LogRecord {
	logRecord := pdata.NewLogRecord()
	pdata.NewAttributeMapFromMap(attr).CopyTo(logRecord.Attributes())
	body.CopyTo(logRecord.Body())
	return logRecord
}

func newResourceLogs(attr map[string]pdata.AttributeValue) pdata.ResourceLogs {
	resourceLogs := pdata.NewResourceLogs()
	pdata.NewAttributeMapFromMap(attr).CopyTo(resourceLogs.Resource().Attributes())
	return resourceLogs
}

func newInstrumentationLibraryLogs(name, version string) pdata.InstrumentationLibraryLogs {
	il := pdata.NewInstrumentationLibraryLogs()
	il.InstrumentationLibrary().SetName(name)
	il.InstrumentationLibrary().SetVersion(version)
	return il
}

func newAttributeMap(attr map[string]pdata.AttributeValue) pdata.AttributeValue {
	m := pdata.NewAttributeValueMap()
	pdata.NewAttributeMapFromMap(attr).CopyTo(m.MapVal())
	return m
}
