# miniDB

A small but fully featured relational database engine, written from scratch in Go.

## Features

- **Disk & file management** — fixed-size block I/O over OS files.
- **Write-ahead logging (WAL)** — append-only log with a reverse iterator for recovery.
- **Buffer management** — page cache with pin/unpin and a replacement strategy.
- **Transactions** — ACID unit of work with two-phase locking (2PL) and undo-based
  recovery (commit / rollback / crash recovery on startup).
- **Record management** — slotted-page record format; `Schema` → `Layout` field offsets.
- **Metadata catalog** — table, field, view, and index catalogs plus statistics, all
  stored as ordinary tables.
- **Indexing** — both **static hash** and **B-tree** indexes.
- **SQL** — a lexer, parser, and planner supporting DDL, DML, and rich `SELECT` queries.
- **Query processing** — relational operators (select, project, product), plus
  materialization, external sorting, grouping, and aggregation.
- **Cost-based query optimization** — the planner uses table statistics and available
  indexes to choose plans (e.g. index scans and index joins).
- **`database/sql` driver** — use miniDB through Go's standard database interface.
- **Interactive REPL** — a shell for running SQL by hand.

## Architecture

Layers, bottom to top. Each layer depends only on the ones below it.

```
                         server.MiniDB  (wiring / bootstrap)
                                  |
        +-------------------------+--------------------------+
        |                         |                          |
   metadata.Manager         planner / operators        transaction.Transaction
   (catalog + stats)        (parse -> plan -> scan)     (ACID unit of work)
        |                          |                          |
        +----------> tablescan.TableScan          +-----------+-----------+
                          (record cursor)         |                       |
                                  |          recovery (WAL)        concurrency (2PL)
                            records.Page          |                       |
                          (slotted records)   log.Manager        concurrency.LockTable
                                  |
                                  +---------> buffer.Manager
                                             (page cache, pin/unpin)
                                                  |
                                             file.Manager
                                          (block I/O, raw Page bytes)
                                                  |
                                                 Disk
```

| Layer | Package(s) | Responsibility |
|-------|-----------|----------------|
| Disk / File | `file` | Fixed-size block I/O; `Page` = raw byte buffer with typed accessors. |
| Log (WAL) | `log` | Append-only write-ahead log; reverse iterator. |
| Buffer | `buffer` | In-memory page cache; pin/unpin; flush dirty pages. |
| Transaction | `transaction`, `transaction/concurrency` | ACID unit of work: 2PL locking + undo logging. |
| Record | `records` | Slotted-page record format; schema → layout. |
| Table cursor | `tablescan` | Iterates records across all blocks of a `.tbl` file. |
| Scan interfaces | `scan` | `Scan` / `UpdateScan` — the relational-operator contract. |
| Query operators | `query` | Select / project / product scans, predicates, sorting, grouping, aggregation. |
| Planning | `plan`, `plan_impl` | `Plan` interface and cost-based plan implementations. |
| Parsing | `parser` | SQL lexer + parser producing query/update data objects. |
| Metadata | `metadata` | System catalog (tables, fields, views, indexes) + statistics. |
| Index | `index`, `index/hash`, `index/btree` | `Index` interface + hash and B-tree implementations. |
| Materialize | `materialize` | Temp tables backing sort/group materialization. |
| Server | `server` | Top-level `MiniDB`: constructs managers, runs recovery, hands out transactions. |
| Driver | `driver` | `database/sql` driver registered as `minidb`. |

A key design choice worth knowing: **the catalog is just tables.** `table_catalog`,
`field_catalog`, `view_catalog`, and `index_catalog` are real tables read and written
through `TableScan` like any user table.

## Requirements

- Go **1.24** or newer.

The only external dependency is [`stretchr/testify`](https://github.com/stretchr/testify),
used in tests.

## Getting started

Clone and build:

```sh
git clone https://github.com/xd-sarthak/miniDB.git
cd miniDB
go build ./...
```

### Run the example

[`example.go`](./example.go) is an end-to-end demo: it creates a table, demonstrates a
rollback and a commit, then runs an `ORDER BY` query.

```sh
go run ./example.go
```

### Run the interactive shell

The REPL opens (or creates) a database directory and reads SQL from stdin:

```sh
go run ./cmd/repl [db-directory]   # defaults to ./minidb-data
```

Type SQL statements terminated by `;` (they may span multiple lines). Dot-commands
control the session:

| Command | Description |
| --- | --- |
| `.help` | list commands |
| `.begin` | start an explicit transaction |
| `.commit` | commit the current transaction |
| `.rollback` | roll back the current transaction |
| `.exit` / `.quit` | leave the shell (rolls back any open transaction) |

`SELECT` statements print a result table; other statements report rows affected. When a
transaction is open, the prompt changes to `minidb*>`.

Example session:

```
minidb> CREATE TABLE student (sname VARCHAR(10), gradyear INT);
OK (0 row(s) affected)
minidb> INSERT INTO student (sname, gradyear) VALUES ('Alice', 2023);
OK (1 row(s) affected)
minidb> SELECT sname, gradyear FROM student ORDER BY gradyear;
sname  gradyear
-----  --------
Alice  2023
(1 row(s))
```

## Using the `database/sql` driver

miniDB registers a driver named `minidb`. Import the package for its side effects and use
the standard library as you would with any SQL database. The data source name is the
directory where database files live.

```go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/xd-sarthak/miniDB/driver" // registers the "minidb" driver
)

func main() {
	db, err := sql.Open("minidb", "./mydb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// miniDB is a single-writer engine; keep database/sql to one connection.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec("CREATE TABLE student (sname VARCHAR(10), gradyear INT)"); err != nil {
		log.Fatal(err)
	}

	// Explicit transaction with commit.
	tx, _ := db.Begin()
	tx.Exec("INSERT INTO student (sname, gradyear) VALUES ('Alice', 2023)")
	tx.Exec("INSERT INTO student (sname, gradyear) VALUES ('Bob', 2024)")
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("SELECT sname, gradyear FROM student ORDER BY gradyear")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var year int
		if err := rows.Scan(&name, &year); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s graduates in %d\n", name, year)
	}
}
```

## Supported SQL

- **DDL:** `CREATE TABLE`, `CREATE VIEW`, `CREATE INDEX`.
- **DML:** `INSERT`, `UPDATE`, `DELETE`.
- **Queries:** `SELECT` with `WHERE`, `GROUP BY`, `HAVING`, and `ORDER BY`.
- **Aggregates:** `min`, `max`, `count`, `avg`, `sum`.
- **Types:** `INT` and `VARCHAR(n)` (the engine also supports additional scalar types
  internally).

The planner uses table statistics and available indexes (hash and B-tree) to optimize
queries, including index selection and index joins.

## Project layout

```
buffer/        page cache (pin/unpin, replacement)
cmd/repl/      interactive SQL shell
driver/        database/sql driver ("minidb")
file/          block I/O and raw pages
index/         Index interface
  hash/        static hash index
  btree/       B-tree index
log/           write-ahead log
materialize/   temp tables for sort/group
metadata/      system catalog + statistics
parser/        SQL lexer and parser
plan/          Plan interface
plan_impl/     cost-based plan implementations
query/         relational operators, predicates, sorting, aggregation
  functions/   aggregation functions
records/       slotted-page record format
scan/          Scan / UpdateScan interfaces
server/        top-level MiniDB bootstrap + recovery
tablescan/     table record cursor
transaction/   ACID transactions
  concurrency/ lock table (2PL)
utils/         shared helpers
example.go     end-to-end demo
```

## Testing

```sh
go test ./...           # full suite
go test -race ./...     # with the race detector
```

## Acknowledgements

The design follows Edward Sciore's *Database Design and Implementation* and its
accompanying **SimpleDB** teaching database, reimagined in Go.
