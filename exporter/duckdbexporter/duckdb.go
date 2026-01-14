package duckdbexporter

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
)

func testDuckdb() {
	db, err := sql.Open("duckdb", "vsfveidevefuncionar.db")

	// ctx := context.Background()

	// sqlConn, err := db.Conn(ctx)

	// if err != nil {
	// 	fmt.Println("error sqlConn", err)
	// } else {
	// 	fmt.Println("tem sqlconn", sqlConn)
	// }

	// connector, err := duckdb.NewConnector("lale.db", nil)

	// if err != nil {
	// 	fmt.Println("!!!!! error on NewConnector", err)
	// } else {
	// 	defer connector.Close()

	// 	fmt.Println("!!!!! connector ok")

	// 	conn, err := connector.Connect(ctx)

	// 	if err != nil {
	// 		fmt.Println("error connector.Connect", err)
	// 	} else {
	// 		defer conn.Close()

	// 		tbls, err := duckdb.GetTableNames(sqlConn, "", false)

	// 		fmt.Println("TABLES??", tbls, err)

	// 		appender, err := duckdb.NewAppenderFromConn(conn, "", "user")

	// 		if err != nil {
	// 			fmt.Println("error appender", err)
	// 		} else {
	// 			defer appender.Close()
	// 			fmt.Println("!!!!deu tudo certo", appender)
	// 		}
	// 	}
	// }

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
		log.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO people VALUES (42, 'John')`)
	if err != nil {
		log.Fatal(err)
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
