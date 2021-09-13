module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/stackdriverexporter

go 1.16

require (
	github.com/open-telemetry/opentelemetry-collector-contrib/exporter/googlecloudexporter v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/collector v0.23.1-0.20210329172532-38e57614135f
	go.uber.org/zap v1.19.1
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/exporter/googlecloudexporter => ../googlecloudexporter
