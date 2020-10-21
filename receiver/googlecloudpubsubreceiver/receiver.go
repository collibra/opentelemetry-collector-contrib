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

package googlecloudpubsubreceiver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	pubsub "cloud.google.com/go/pubsub/apiv1"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/otlp"
	"go.opentelemetry.io/collector/model/pdata"
	"go.opentelemetry.io/collector/obsreport"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudpubsubreceiver/internal"
)

// https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#streamingpullrequest
type pubsubReceiver struct {
	logger             *zap.Logger
	obsrecv            *obsreport.Receiver
	tracesConsumer     consumer.Traces
	metricsConsumer    consumer.Metrics
	logsConsumer       consumer.Logs
	userAgent          string
	config             *Config
	client             *pubsub.SubscriberClient
	tracesUnmarshaler  pdata.TracesUnmarshaler
	metricsUnmarshaler pdata.MetricsUnmarshaler
	logsUnmarshaler    pdata.LogsUnmarshaler
	handler            internal.StreamHandler
	stopOnce           sync.Once
	startOnce          sync.Once
}

type Encoding int

const (
	Unknown         = iota
	OtlpProtoTrace  = iota
	OtlpProtoMetric = iota
	OtlpProtoLog    = iota
	RawTextLog      = iota
)

func (receiver *pubsubReceiver) generateClientOptions() ([]option.ClientOption, error) {
	var copts []option.ClientOption
	if receiver.userAgent != "" {
		copts = append(copts, option.WithUserAgent(receiver.userAgent))
	}
	if receiver.config.Endpoint != "" {
		if receiver.config.Insecure {
			var dialOpts []grpc.DialOption
			if receiver.userAgent != "" {
				dialOpts = append(dialOpts, grpc.WithUserAgent(receiver.userAgent))
			}
			conn, _ := grpc.Dial(receiver.config.Endpoint, append(dialOpts, grpc.WithInsecure())...)
			copts = append(copts, option.WithGRPCConn(conn))
		} else {
			copts = append(copts, option.WithEndpoint(receiver.config.Endpoint))
		}
	}
	return copts, nil
}

func (receiver *pubsubReceiver) Start(ctx context.Context, _ component.Host) error {
	if receiver.tracesConsumer == nil && receiver.metricsConsumer == nil && receiver.logsConsumer == nil {
		return errors.New("cannot start receiver: no consumers were specified")
	}

	var err error
	receiver.startOnce.Do(func() {
		copts, _ := receiver.generateClientOptions()
		client, err := pubsub.NewSubscriberClient(ctx, copts...)
		if err != nil {
			err = fmt.Errorf("failed creating the gRPC client to Pubsub: %w", err)
			return
		}
		receiver.client = client

		err = receiver.createReceiverHandler(ctx)
		if err != nil {
			err = fmt.Errorf("failed to create ReceiverHandler: %w", err)
			return
		}
	})
	receiver.tracesUnmarshaler = otlp.NewProtobufTracesUnmarshaler()
	receiver.metricsUnmarshaler = otlp.NewProtobufMetricsUnmarshaler()
	receiver.logsUnmarshaler = otlp.NewProtobufLogsUnmarshaler()
	return err
}

func (receiver *pubsubReceiver) Shutdown(_ context.Context) error {
	receiver.logger.Info("Stopping Google Pubsub receiver")
	receiver.handler.CancelNow()
	receiver.logger.Info("Stopped Google Pubsub receiver")
	return nil
}

func (receiver *pubsubReceiver) handleLogStrings(ctx context.Context, message *pubsubpb.ReceivedMessage) error {
	if receiver.logsConsumer == nil {
		return nil
	}
	data := string(message.Message.Data)
	timestamp := message.GetMessage().PublishTime

	out := pdata.NewLogs()
	logs := out.ResourceLogs()
	logs.Resize(1)
	rls := logs.At(0)
	// TODO can we extract some attributes to add to the instrumentation library

	rls.InstrumentationLibraryLogs().Resize(1)
	ills := rls.InstrumentationLibraryLogs().At(0)

	ills.Logs().Resize(1)
	lr := ills.Logs().At(0)
	// TODO can we extract some attributes to add to the log line

	lr.Body().SetStringVal(data)
	lr.SetTimestamp(pdata.TimestampFromTime(timestamp.AsTime()))
	return receiver.logsConsumer.ConsumeLogs(ctx, out)
}

func (receiver *pubsubReceiver) detectEncoding(attributes map[string]string) (Encoding, error) {
	if receiver.config.Encoding != "" {
		switch receiver.config.Encoding {
		case "otlp_proto_trace":
			return OtlpProtoTrace, nil
		case "otlp_proto_metric":
			return OtlpProtoMetric, nil
		case "otlp_proto_log":
			return OtlpProtoLog, nil
		case "raw_text":
			return RawTextLog, nil
		}
	}

	ceType := attributes["ce-type"]
	ceContentType := attributes["content-type"]
	if strings.HasSuffix(ceContentType, "application/protobuf") {
		switch ceType {
		case "org.opentelemetry.otlp.traces.v1":
			return OtlpProtoTrace, nil
		case "org.opentelemetry.otlp.metrics.v1":
			return OtlpProtoMetric, nil
		case "org.opentelemetry.otlp.logs.v1":
			return OtlpProtoLog, nil
		}
	} else if strings.HasSuffix(ceContentType, "text/plain") {
		return RawTextLog, nil
	}
	return Unknown, nil
}

func (receiver *pubsubReceiver) createReceiverHandler(ctx context.Context) error {
	var err error
	receiver.handler, err = internal.NewHandler(
		ctx,
		receiver.logger,
		receiver.client,
		receiver.config.ClientID,
		receiver.config.Subscription,
		func(ctx context.Context, message *pubsubpb.ReceivedMessage) error {

			ctx = obsreport.ReceiverContext(ctx, receiver.config.ID(), reportTransport)
			encoding, err := receiver.detectEncoding(message.Message.Attributes)
			switch encoding {
			case OtlpProtoTrace:
				if receiver.tracesConsumer != nil {
					otlpData, err := receiver.tracesUnmarshaler.UnmarshalTraces(message.Message.Data)
					count := otlpData.SpanCount()
					if err != nil {
						return err
					}
					ctx = receiver.obsrecv.StartTracesOp(ctx)
					err = receiver.tracesConsumer.ConsumeTraces(ctx, otlpData)
					receiver.obsrecv.EndTracesOp(ctx, reportFormatProtobuf, count, err)
				}
			case OtlpProtoMetric:
				if receiver.metricsConsumer != nil {
					otlpData, err := receiver.metricsUnmarshaler.UnmarshalMetrics(message.Message.Data)
					count := otlpData.MetricCount()
					if err != nil {
						return err
					}
					ctx = receiver.obsrecv.StartMetricsOp(ctx)
					err = receiver.metricsConsumer.ConsumeMetrics(ctx, otlpData)
					receiver.obsrecv.EndMetricsOp(ctx, reportFormatProtobuf, count, err)
				}
			case OtlpProtoLog:
				if receiver.logsConsumer != nil {
					otlpData, err := receiver.logsUnmarshaler.UnmarshalLogs(message.Message.Data)
					count := otlpData.LogRecordCount()
					if err != nil {
						return err
					}
					ctx = receiver.obsrecv.StartLogsOp(ctx)
					err = receiver.logsConsumer.ConsumeLogs(ctx, otlpData)
					receiver.obsrecv.EndLogsOp(ctx, reportFormatProtobuf, count, err)
				}
			case RawTextLog:
				return receiver.handleLogStrings(ctx, message)
			}
			return err
		})
	if err != nil {
		return err
	}
	go receiver.handler.RecoverableStream(ctx)
	return nil
}
