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

package googlecloudpubsubexporter

import (
	"context"
	"fmt"
	"sync"

	pubsub "cloud.google.com/go/pubsub/apiv1"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc"
)

const name = "googlecloudpubsub"

// pubsubExporter is a wrapper struct of OT cloud trace exporter
type pubsubExporter struct {
	instanceName string
	logger       *zap.Logger

	tracesTopicName  string
	metricsTopicName string
	logsTopicName    string

	client *pubsub.PublisherClient

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

func (ex *pubsubExporter) Start(ctx context.Context, _ component.Host) error {
	ctx, ex.cancel = context.WithCancel(ctx)

	if ex.client == nil {
		copts, _ := ex.generateClientOptions()
		client, err := pubsub.NewPublisherClient(ctx, copts...)
		if err != nil {
			return fmt.Errorf("failed creating the gRPC client to Pubsub: %w", err)
		}

		ex.client = client
	}
	return nil
}

func (ex *pubsubExporter) Shutdown(context.Context) error {
	if ex.client != nil {
		ex.client.Close()
		ex.client = nil
	}
	return nil
}

func (ex *pubsubExporter) generateClientOptions() ([]option.ClientOption, error) {
	var copts []option.ClientOption
	if ex.userAgent != "" {
		copts = append(copts, option.WithUserAgent(ex.userAgent))
	}
	if ex.config.Endpoint != "" {
		if ex.config.UseInsecure {
			var dialOpts []grpc.DialOption
			if ex.userAgent != "" {
				dialOpts = append(dialOpts, grpc.WithUserAgent(ex.userAgent))
			}
			conn, _ := grpc.Dial(ex.config.Endpoint, append(dialOpts, grpc.WithInsecure())...)
			copts = append(copts, option.WithGRPCConn(conn))
		} else {
			copts = append(copts, option.WithEndpoint(ex.config.Endpoint))
		}
	}
	return copts, nil
}

func (ex *pubsubExporter) ConsumeTraces(ctx context.Context, td pdata.Traces) error {
	bytes, _ := td.ToOtlpProtoBytes()
	_, err := ex.client.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: ex.tracesTopicName,
		Messages: []*pubsubpb.PubsubMessage{
			{
				Data: bytes,
			},
		},
	})
	return err
}

func (ex *pubsubExporter) ConsumeMetrics(ctx context.Context, td pdata.Metrics) error {
	bytes, _ := td.ToOtlpProtoBytes()
	_, err := ex.client.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: ex.metricsTopicName,
		Messages: []*pubsubpb.PubsubMessage{
			{
				Data: bytes,
			},
		},
	})
	return err
}

func (ex *pubsubExporter) ConsumeLogs(ctx context.Context, td pdata.Logs) error {
	bytes, _ := td.ToOtlpProtoBytes()
	_, err := ex.client.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: ex.logsTopicName,
		Messages: []*pubsubpb.PubsubMessage{
			{
				Data: bytes,
			},
		},
	})
	return err
}
