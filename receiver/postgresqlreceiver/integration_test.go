// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package postgresqlreceiver

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/collector/component"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/testutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/scraperinttest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
)

const postgresqlPort = "5432"

func TestIntegration(t *testing.T) {
	defer testutil.SetFeatureGateForTest(t, separateSchemaAttrGate, false)()
	defer testutil.SetFeatureGateForTest(t, connectionPoolGate, false)()
	t.Run("single_db", integrationTest(t, "single_db", []string{"otel"}))
	t.Run("multi_db", integrationTest(t, "multi_db", []string{"otel", "otel2"}))
	t.Run("all_db", integrationTest(t, "all_db", []string{}))
}

func TestIntegrationWithSeparateSchemaAttr(t *testing.T) {
	defer testutil.SetFeatureGateForTest(t, separateSchemaAttrGate, true)()
	defer testutil.SetFeatureGateForTest(t, connectionPoolGate, false)()
	t.Run("single_db_schemaattr", integrationTest(t, "single_db_schemaattr", []string{"otel"}))
	t.Run("multi_db_schemaattr", integrationTest(t, "multi_db_schemaattr", []string{"otel", "otel2"}))
	t.Run("all_db_schemaattr", integrationTest(t, "all_db_schemaattr", []string{}))
}

func TestIntegrationWithConnectionPool(t *testing.T) {
	defer testutil.SetFeatureGateForTest(t, connectionPoolGate, true)()
	t.Run("single_db", integrationTest(t, "single_db_pooledconn", []string{"otel"}))
	t.Run("multi_db", integrationTest(t, "multi_db_pooledconn", []string{"otel", "otel2"}))
	t.Run("all_db", integrationTest(t, "all_db_pooledconn", []string{}))
}

func integrationTest(tb testing.TB, name string, databases []string) func(*testing.T) {
	expectedFile := filepath.Join("testdata", "integration", "expected_"+name+".yaml")
	return scraperinttest.NewIntegrationTest(
		NewFactory(),
		scraperinttest.WithContainerRequest(
			testcontainers.ContainerRequest{
				Image: "postgres:12.17",
				Cmd: []string{
					"postgres",
					"-c", "log_connections=on",
					"-c", "log_disconnections=on",
				},
				Env: map[string]string{
					"POSTGRES_USER":     "root",
					"POSTGRES_PASSWORD": "otel",
					"POSTGRES_DB":       "otel",
				},
				Files: []testcontainers.ContainerFile{{
					HostFilePath:      filepath.Join("testdata", "integration", "init.sql"),
					ContainerFilePath: "/docker-entrypoint-initdb.d/init.sql",
					FileMode:          700,
				}},
				ExposedPorts: []string{postgresqlPort},
				WaitingFor: wait.ForListeningPort(postgresqlPort).
					WithStartupTimeout(2 * time.Minute),
				/*LifecycleHooks: []testcontainers.ContainerLifecycleHooks{{
					PreTerminates: []testcontainers.ContainerHook{
						func(ctx context.Context, container testcontainers.Container) error {
							logReader, err := container.Logs(ctx)
							if err != nil {
								return err
							}
							logs, err := io.ReadAll(logReader)
							if err != nil {
								return err
							}
							tb.Logf("Container logs %q:\n%s\n------\n", container.GetContainerID(), string(logs))
							return nil
						},
					},
				}},*/
			}),
		scraperinttest.WithCustomConfig(
			func(t *testing.T, cfg component.Config, ci *scraperinttest.ContainerInfo) {
				rCfg := cfg.(*Config)
				rCfg.CollectionInterval = time.Second
				rCfg.Endpoint = net.JoinHostPort(ci.Host(t), ci.MappedPort(t, postgresqlPort))
				rCfg.Databases = databases
				rCfg.Username = "otelu"
				rCfg.Password = "otelp"
				rCfg.Insecure = true
				rCfg.Metrics.PostgresqlDeadlocks.Enabled = true
				rCfg.Metrics.PostgresqlTempFiles.Enabled = true
				rCfg.Metrics.PostgresqlSequentialScans.Enabled = true
				rCfg.Metrics.PostgresqlDatabaseLocks.Enabled = true
			}),
		scraperinttest.WithExpectedFile(expectedFile),
		scraperinttest.WithCompareOptions(
			pmetrictest.IgnoreResourceMetricsOrder(),
			pmetrictest.IgnoreMetricValues(),
			pmetrictest.IgnoreSubsequentDataPoints("postgresql.backends", "postgresql.database.locks"),
			pmetrictest.IgnoreMetricDataPointsOrder(),
			pmetrictest.IgnoreStartTimestamp(),
			pmetrictest.IgnoreTimestamp(),
		),
	).Run
}
