module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/googlecloudpubsubexporter

go 1.14

require (
	cloud.google.com/go/pubsub v1.10.1
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/collector v0.23.1-0.20210406143646-80a415db430f
	go.uber.org/zap v1.16.0
	google.golang.org/api v0.43.0
	google.golang.org/genproto v0.0.0-20210319143718-93e7006c17a6
	google.golang.org/grpc v1.36.1
)
