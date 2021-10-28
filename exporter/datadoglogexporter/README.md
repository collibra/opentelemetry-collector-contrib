# Datadog Log Exporter

This exporter can be used to send logs to [Datadog](https://datadoghq.com).

> **Note**: The log exporter is not considered stable yet and may suffer breaking changes.

## Configuration

The only required settings are:
- [Datadog Log intake URL](https://docs.datadoghq.com/api/latest/logs/#send-logs)
- [Datadog API key](https://app.datadoghq.com/account/settings#api)

```yaml
datadoglog:
  url: "https://http-intake.logs.datadoghq.com/v1/input"
  apiKey: "${DATADOG_APIKEY}"
```

In this example, the `apiKey` is injected using [environment variables](https://opentelemetry.io/docs/collector/configuration/#configuration-environment-variables).