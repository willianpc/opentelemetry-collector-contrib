package duckdbexporter

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
	"go.uber.org/zap"
)

const (
	tracesTable = iota
	logsTable
	metricsTable
)

const createTrcesTable = `CREATE TABLE %s (
	service_name VARCHAR,
    name VARCHAR,
    id VARCHAR primary key,
    parent_id VARCHAR,
    trace_id VARCHAR,
    kind VARCHAR,
    schema_url VARCHAR,
    resources map(VARCHAR, VARCHAR),
    scope_name VARCHAR,
    scope_version VARCHAR,
    start_timestamp TIMESTAMP,
	end_timestamp TIMESTAMP,
    flags UINTEGER,
    status_code VARCHAR,
    status_message VARCHAR,

	event_times TIMESTAMP[],
    event_names VARCHAR[],
    event_attrs map(VARCHAR, VARCHAR)[],

    link_trace_ids VARCHAR[],
    links_span_ids VARCHAR[],
    links_trace_states VARCHAR[],
    links_attrs map(VARCHAR, VARCHAR)[]
);
`

const createLogsTable = `CREATE TABLE %s (
	span_id VARCHAR,
	trace_id VARCHAR,
	scope VARCHAR,
	timestamp TIMESTAMP
);`

func acquireAppenderForTable(cfg *Config, logger *zap.Logger, table int) (*duckdb.Appender, func(), error) {
	var tableName, createTableQuery string

	switch table {
	case tracesTable:
		tableName = cfg.TracesTableName
		createTableQuery = createTrcesTable
	case logsTable:
		tableName = cfg.LogsTableName
		createTableQuery = createLogsTable
	default:
		tableName = ""
	}

	connector, err := duckdb.NewConnector(cfg.DatabaseName, nil)
	if err != nil {
		return nil, func() {}, err
	}
	// defer connector.Close()

	conn, err := connector.Connect(context.Background())
	if err != nil {
		return nil, func() {}, err
	}
	// defer conn.Close()

	stmt, err := conn.Prepare(fmt.Sprintf(createTableQuery, tableName))
	if err != nil {
		logger.Error("Error preparing statement")
	}

	defer stmt.Close()

	_, err = stmt.Exec([]driver.Value{})
	if err != nil {
		logger.Info(fmt.Sprintf("Error on stmt: %v", err))
	}

	// Retrieve appender from connection (note that you have to create the table beforehand).
	appender, err := duckdb.NewAppenderFromConn(conn, "", tableName)
	if err != nil {
		return nil, func() {}, err
	}

	closeDbConnectionsFn := func() {
		if err = conn.Close(); err != nil {
			logger.Error(fmt.Sprintf("Error closing driver.Conn: %v", err))
		}

		if err = connector.Close(); err != nil {
			logger.Error(fmt.Sprintf("Error closing *duckdb.Connector: %v", err))
		}

		if err = appender.Close(); err != nil {
			logger.Error(fmt.Sprintf("Error closing *duckdb.Appender: %v", err))
		}
	}

	return appender, closeDbConnectionsFn, nil
}
