package internal

import (
	cloudlogging "cloud.google.com/go/logging/apiv2"
	"context"
	"fmt"
	"go.opentelemetry.io/collector/consumer/pdata"
	monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	loggingpb "google.golang.org/genproto/googleapis/logging/v2"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
)

type GoogleLogging struct {
	ProjectID string
	LogName string
	client *cloudlogging.Client
}

func NewGoogleLogging(ctx context.Context, projectID string) *GoogleLogging {
	client, _ := cloudlogging.NewClient(ctx)

	return &GoogleLogging {
		ProjectID: projectID,
		client: client,
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
	fmt.Printf("NewBatch %v\n",0)
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
		fmt.Printf("%v\n", l)
		// ignoring for now: l.InstrumentationLibrary()
		logSlice := l.Logs()

		for j := 0; j < logSlice.Len(); j++ {
			logEnties = append(logEnties, gl.ToLogEntry(logSlice.At(j)))
		}

		//rl := te.lexporter.ToLogEntries(l.)
		//logEntries = append(logEntries, rl...)
	}

	logs.InstrumentationLibraryLogs()

	return logEnties

}

func (gl *GoogleLogging) addBodyToPayload(entry *loggingpb.LogEntry, value pdata.AttributeValue) error {
	switch value.Type() {
	case pdata.AttributeValueSTRING:
		entry.Payload = &loggingpb.LogEntry_TextPayload{
			TextPayload: value.StringVal(),
		}
		break
	case pdata.AttributeValueINT:
		break
	case pdata.AttributeValueDOUBLE:
		break
	case pdata.AttributeValueBOOL:
		break
	case pdata.AttributeValueMAP:
		json, _ := gl.attributeMapToStruct(value.MapVal())
		entry.Payload = &loggingpb.LogEntry_JsonPayload{
			JsonPayload: json,
		}
		break
	case pdata.AttributeValueARRAY:
		break
	case pdata.AttributeValueNULL:
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
	if flags & 0x00000001 == 0x00000001 {
		entry.TraceSampled = true
	}
}

func (gl *GoogleLogging) attributeMapToStruct(val pdata.AttributeMap) (*structpb.Struct, error) {
	entries := make(map[string]interface{}, val.Len())
	val.ForEach(func(k string, v pdata.AttributeValue) {
		entries[k] = v.StringVal()
	})
	return structpb.NewStruct(entries)
}

func (gl *GoogleLogging) ToLogEntry(logRecord pdata.LogRecord) *loggingpb.LogEntry {
	entry := &loggingpb.LogEntry{
		Timestamp: &timestamppb.Timestamp{
			Seconds: int64(logRecord.Timestamp().AsTime().Unix()),
			Nanos:   int32(logRecord.Timestamp().AsTime().Nanosecond()),
		},
		LogName:  gl.LogName,
		Resource: &monitoredres.MonitoredResource{Type: "global"},
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
