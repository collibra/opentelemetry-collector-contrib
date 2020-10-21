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

package internal

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	pubsub "cloud.google.com/go/pubsub/apiv1"
	"go.uber.org/zap"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#streamingpullrequest
type StreamHandler struct {
	stream      pubsubpb.Subscriber_StreamingPullClient
	pushMessage func(ctx context.Context, message *pubsubpb.ReceivedMessage) error
	acks        []string
	mutex       sync.Mutex
	client      *pubsub.SubscriberClient

	clientId     string
	subscription string

	cancel            context.CancelFunc
	wg                sync.WaitGroup
	receiverWaitGroup sync.WaitGroup
	logger            *zap.Logger

	isRunning bool
}

func (handler *StreamHandler) ack(ackId string) {
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	handler.acks = append(handler.acks, ackId)
}

// When sending a message though the pipeline fails, we ignore the error. We'll let Pubsub
// handle the flow control.
func (handler *StreamHandler) nak(id string, err error) {
}

func NewHandler(
	ctx context.Context,
	logger *zap.Logger,
	client *pubsub.SubscriberClient,
	clientId string,
	subscription string,
	callback func(ctx context.Context, message *pubsubpb.ReceivedMessage) error) (StreamHandler, error) {

	handler := StreamHandler{
		logger:       logger,
		client:       client,
		clientId:     clientId,
		subscription: subscription,
		pushMessage:  callback,
		acks:         make([]string, 0),
	}
	return handler, handler.initStream(ctx)
}

func (handler *StreamHandler) initStream(ctx context.Context) error {
	var err error
	// Create a stream, but with the receivers context as we don't want to cancel and ongoing operation
	handler.stream, err = handler.client.StreamingPull(ctx)
	if err != nil {
		return err
	}

	request := pubsubpb.StreamingPullRequest{
		Subscription:             handler.subscription,
		StreamAckDeadlineSeconds: 60,
		ClientId:                 handler.clientId,
		// TODO: Revisit setting outstanding messages, how thus this inpact concurrency amongst pods
		//MaxOutstandingMessages:   20,
	}
	if err := handler.stream.Send(&request); err != nil {
		_ = handler.stream.CloseSend()
		return err
	}
	return nil
}

func (handler *StreamHandler) RecoverableStream(ctx context.Context) {
	handler.receiverWaitGroup.Add(1)
	handler.isRunning = true
	for handler.isRunning {
		// Create a new cancelable context for the handler, so we can recover the stream
		var handlerCtx context.Context
		handlerCtx, handler.cancel = context.WithCancel(ctx)

		handler.logger.Info("Starting Streaming Pull")
		handler.wg.Add(2)
		go handler.requestStream(handlerCtx)
		go handler.responseStream(handlerCtx)

		select {
		case <-handlerCtx.Done():
			handler.wg.Wait()
		case <-ctx.Done():
		}
		if handler.isRunning {
			err := handler.initStream(ctx)
			if err != nil {
				// TODO, can't re-init, crash?!
				handler.logger.Error("Suicide")
				os.Exit(3)
			}
		}
		handler.logger.Warn("End of recovery loop, restarting.")
		// TODO, make this more intelligent
		time.Sleep(250)
	}
	handler.logger.Warn("Shutting down recovery loop.")
	handler.receiverWaitGroup.Done()
}

func (handler *StreamHandler) CancelNow() {
	handler.isRunning = false
	if handler.cancel != nil {
		handler.cancel()
	}
	handler.receiverWaitGroup.Wait()
}

func (handler *StreamHandler) acknowledgeMessages() error {
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	if len(handler.acks) == 0 {
		return nil
	}
	request := pubsubpb.StreamingPullRequest{
		AckIds: handler.acks,
	}
	handler.acks = make([]string, 0)
	return handler.stream.Send(&request)
}

func (handler *StreamHandler) requestStream(ctx context.Context) {
	duration := 10000 * time.Millisecond
	timer := time.NewTimer(duration)
	for {
		if err := handler.acknowledgeMessages(); err != nil {
			if err == io.EOF {
				handler.logger.Warn("EOF reached")
				break
			}
			// TODO: When can we not ack messages?
			// TODO: For now we continue the loop and hope for the best
			fmt.Println("Failed in acknowledge messages with error", err)
			break
		}
		select {
		case <-ctx.Done():
			handler.logger.Warn("requestStream <-ctx.Done()")
		case <-timer.C:
			timer.Reset(duration)
		}
		if ctx.Err() == context.Canceled {
			_ = handler.acknowledgeMessages()
			timer.Stop()
			break
		}
	}
	handler.cancel()
	handler.logger.Warn("Request Stream loop ended.")
	_ = handler.stream.CloseSend()
	handler.wg.Done()
}

func (handler *StreamHandler) responseStream(ctx context.Context) {
	activeStreaming := true
	for activeStreaming {
		// block until the next message or timeout expires
		resp, err := handler.stream.Recv()
		if err == nil {
			for _, message := range resp.ReceivedMessages {
				// handle all the messages in the response, could be one or more
				err := handler.pushMessage(context.Background(), message)
				if err != nil {
					handler.nak(message.AckId, err)
				}
				handler.ack(message.AckId)
			}
		} else {
			var status, grpcStatus = status.FromError(err)
			switch {
			case err == io.EOF:
				activeStreaming = false
			case !grpcStatus:
				handler.logger.Warn("response stream breaking on error",
					zap.Error(err))
				activeStreaming = false
			case status.Code() == codes.Unavailable:
				handler.logger.Info("response stream breaking on gRPC status 'Unavailable'")
				activeStreaming = false
			case status.Code() == codes.NotFound:
				handler.logger.Error("resource doesn't exist, wait 60 seconds, and restarting stream")
				time.Sleep(time.Millisecond * 60)
				activeStreaming = false
			default:
				handler.logger.Warn(fmt.Sprintf("response stream breaking on gRPC status %s", status.Message()),
					zap.String("status", status.Message()),
					zap.Error(err))
				activeStreaming = false
			}
		}
		if ctx.Err() == context.Canceled {
			// Cancelling the loop, collector is probably stopping
			handler.logger.Warn("response stream ctx.Err() == context.Canceled")
			break
		}
	}
	handler.cancel()
	handler.logger.Warn("Response Stream loop ended.")
	handler.wg.Done()
}
