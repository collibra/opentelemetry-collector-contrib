// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package postgresqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/postgresqlreceiver"

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/lib/pq"
	"go.opentelemetry.io/collector/featuregate"
)

const (
	defaultPostgreSQLDatabase = "postgres"
	connectionPoolGateID      = "receiver.postgresql.connectionPool"
)

var (
	connectionPoolGate = featuregate.GlobalRegistry().MustRegister(
		connectionPoolGateID,
		featuregate.StageAlpha,
		featuregate.WithRegisterDescription("Use of connection pooling"),
		featuregate.WithRegisterReferenceURL("https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/30831"),
	)
)

type postgreSQLClientFactory interface {
	getClient(ctx context.Context, database string) (client, error)
	close() error
}

// defaultClientFactory creates one connection on each call, using lib/pq module
type defaultClientFactory struct {
	baseConfig postgreSQLConfig
}

func newDefaultClientFactory(cfg *Config) postgreSQLClientFactory {
	return &defaultClientFactory{
		baseConfig: postgreSQLConfig{
			username: cfg.Username,
			password: string(cfg.Password),
			database: defaultPostgreSQLDatabase, // use this as default for e.g. listing all databases
			address:  cfg.NetAddr,
			tls:      cfg.TLSClientSetting,
		},
	}
}

func (d *defaultClientFactory) getClient(_ context.Context, database string) (client, error) {
	cfg := d.baseConfig
	if database != "" {
		cfg.database = database
	}
	connectionString, err := cfg.ConnectionString()
	if err != nil {
		return nil, err
	}
	conn, err := pq.NewConnector(connectionString)
	if err != nil {
		return nil, err
	}
	return newPostgreSQLClient(sql.OpenDB(conn)), nil
}

func (d *defaultClientFactory) close() error {
	return nil
}

// pooledClientFactory creates a connection pool once and reuses it, using jackx/pgx module
type pooledClientFactory struct {
	baseConfig postgreSQLConfig
	pools      map[string]*pgxpool.Pool
}

func newPooledClientFactory(cfg *Config) postgreSQLClientFactory {
	return &pooledClientFactory{
		baseConfig: postgreSQLConfig{
			username: cfg.Username,
			password: string(cfg.Password),
			database: defaultPostgreSQLDatabase, // use this as default for e.g. listing all databases
			address:  cfg.NetAddr,
			tls:      cfg.TLSClientSetting,
		},
		pools: map[string]*pgxpool.Pool{},
	}
}

func (p *pooledClientFactory) newPool(ctx context.Context, database string) (*pgxpool.Pool, error) {
	cfg := p.baseConfig
	if database != "" {
		cfg.database = database
	}
	connectionString, err := cfg.ConnectionString()
	if err != nil {
		return nil, err
	}
	return pgxpool.New(ctx, connectionString)
}

func (p *pooledClientFactory) getClient(ctx context.Context, database string) (client, error) {
	pool, ok := p.pools[database]
	if !ok {
		var err error
		pool, err = p.newPool(ctx, database)
		if err != nil {
			return nil, err
		}
		p.pools[database] = pool
	}
	return newPostgreSQLClient(stdlib.OpenDBFromPool(pool)), nil
}

func (p *pooledClientFactory) close() error {
	if len(p.pools) == 0 {
		return nil
	}
	for _, pool := range p.pools {
		pool.Close()
	}
	return nil
}
