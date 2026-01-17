package duckdbexporter

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
	"go.uber.org/zap"
)

const createSpansTable = `CREATE TABLE %s (
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

func withAppender(logger *zap.Logger, dbName string, traceTableName string) (*duckdb.Appender, func(), error) {
	connector, err := duckdb.NewConnector(dbName, nil)

	if err != nil {
		return nil, func() {}, err
	}
	// defer connector.Close()

	conn, err := connector.Connect(context.Background())
	if err != nil {
		return nil, func() {}, err
	}
	// defer conn.Close()

	stmt, err := conn.Prepare(fmt.Sprintf(createSpansTable, traceTableName))

	if err != nil {
		logger.Error("Error preparing statement")
	}

	defer stmt.Close()

	_, err = stmt.Exec([]driver.Value{})

	if err != nil {
		logger.Error(fmt.Sprintf("Error on stmt: %v", err))
	}

	// Retrieve appender from connection (note that you have to create the table 'test' beforehand).
	appender, err := duckdb.NewAppenderFromConn(conn, "", traceTableName)
	if err != nil {
		return nil, func() {}, err
	}
	// defer appender.Close()

	return appender, func() {
		err = conn.Close()

		if err != nil {
			panic(err)
		}

		err = connector.Close()

		if err != nil {
			panic(err)
		}

		err = appender.Close()

		if err != nil {
			panic(err)
		}

	}, nil
}

func duckdbMapFromStringMap(m map[string]string) duckdb.Map {
	ddbm := make(duckdb.Map)

	for k, v := range m {
		ddbm[k] = v
	}

	return ddbm
}
