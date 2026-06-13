# miniDB 
## A simple, fully featured database written in Go.
This project is built as a personal exercise as I work my way through database implementation concepts.

The database implements the following features:
- [x] Disk and File Management
- [x] Memory Management
- [x] Transaction Management
- [x] Record Management
- [x] Metadata Management
- [x] Query Processing
- [x] Query Parsing
- [x] Parsing
- [x] Planning
- [x] JDBC Interfaces
- [x] Indexing
- [x] Materialization and Sorting
- [x] Effective Buffer Utilization
- [x] Query Optimization

## Usage

miniDB ships a `database/sql` driver (registered as `minidb`). See
[`example.go`](./example.go) for an end-to-end demo (`go run ./example.go`),
which creates a table, demonstrates commit/rollback, and runs an `ORDER BY`
query. SQL supported includes `CREATE TABLE/VIEW/INDEX`, `INSERT`, `UPDATE`,
`DELETE`, and `SELECT` with `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, and the
`min`/`max`/`count`/`avg`/`sum` aggregates. The planner uses table statistics
and available indexes (hash and B-tree) for query optimization.
