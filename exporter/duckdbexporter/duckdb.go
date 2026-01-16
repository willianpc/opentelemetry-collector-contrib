package duckdbexporter

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"

	"github.com/duckdb/duckdb-go/v2"
)

const createSpansTable = `CREATE TABLE %s (
    name VARCHAR,
    id VARCHAR primary key,
    parent_id VARCHAR,
    trace_id VARCHAR,
    kind UINTEGER,
    schema_url VARCHAR,
    resources map(VARCHAR, VARCHAR),
    resource_scope VARCHAR,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    flags UINTEGER,

    event_times TIMESTAMP[],
    event_names VARCHAR[],
    event_attrs map(VARCHAR, VARCHAR),

    link_trace_ids VARCHAR[],
    links_span_ids VARCHAR[],
    links_trace_states UINTEGER[],
    links_attrs map(VARCHAR, VARCHAR)
);`

func testDuckdb() {
	db, err := sql.Open("duckdb", "duckdb-test.db")

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("show tables")

	var res string

	if err != nil {
		fmt.Println("cannot get .tables with error", err)
	} else {
		defer rows.Close()

		fmt.Println(rows.Columns())

		for rows.Next() {
			err := rows.Scan(&res)

			if err != nil {
				fmt.Println("scan error", err)
			} else {
				fmt.Printf("'%s'\n", res)
			}
		}
	}

	_, err = db.Exec(`CREATE TABLE people (id INTEGER, name VARCHAR)`)
	if err != nil {
		fmt.Println(err)
	}
	_, err = db.Exec(`INSERT INTO people VALUES (42, 'John')`)
	if err != nil {
		fmt.Println(err)
	}

	var (
		id   int
		name string
	)
	row := db.QueryRow(`SELECT id, name FROM people`)
	err = row.Scan(&id, &name)
	if errors.Is(err, sql.ErrNoRows) {
		log.Println("no rows")
	} else if err != nil {
		log.Fatal(err)
	}

	// \033[3;36m duckdb :: \033[0m

	fmt.Printf("\033[3;36m id: %d, name: %s \033[0m \n", id, name)
}

func withAppender(dbName string, traceTableName string) {
	connector, err := duckdb.NewConnector(dbName, nil)

	if err != nil {
		fmt.Println("error", err)
	}
	defer connector.Close()

	conn, err := connector.Connect(context.Background())
	if err != nil {
		fmt.Println("error", err)
	}
	defer conn.Close()

	stmt, _ := conn.Prepare(fmt.Sprintf(createSpansTable, traceTableName))
	_, err = stmt.Exec([]driver.Value{})

	if err != nil {
		fmt.Println("error on stmt", err)
	}
	defer stmt.Close()

	// Retrieve appender from connection (note that you have to create the table 'test' beforehand).
	appender, err := duckdb.NewAppenderFromConn(conn, "", traceTableName)
	if err != nil {
		fmt.Println("error", err)
	}
	defer func() {
		appender.Close()
		fmt.Println("appender closed, all good")
	}()

	// err = appender.AppendRow(1, "Mark")

	// if err != nil {
	// 	fmt.Println("appendRow failed", err)
	// }
}
