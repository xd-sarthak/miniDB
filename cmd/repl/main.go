// Command repl is an interactive shell for miniDB.
//
// It opens (or creates) a database directory and reads SQL statements from
// stdin, executing each one against the embedded `minidb` database/sql driver.
// Statements are terminated by a semicolon and may span multiple lines.
//
// Usage:
//
//	go run ./cmd/repl [db-directory]
//
// If no directory is given, "./minidb-data" is used. Dot-commands (.help,
// .begin, .commit, .rollback, .exit) control the session; see .help.
package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	_ "github.com/xd-sarthak/miniDB/driver" // register the "minidb" driver
)

const defaultDir = "./minidb-data"

// session holds the open database and an optional explicit transaction.
// When tx is non-nil, statements are routed through it instead of auto-commit.
type session struct {
	db *sql.DB
	tx *sql.Tx
}

func main() {
	dir := defaultDir
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}

	db, err := sql.Open("minidb", dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open miniDB at %q: %v\n", dir, err)
		os.Exit(1)
	}
	defer db.Close()

	// miniDB is an embedded, single-writer engine, so confine database/sql to
	// one underlying connection to keep transaction state coherent.
	db.SetMaxOpenConns(1)

	s := &session{db: db}

	fmt.Printf("miniDB interactive shell — data directory: %s\n", dir)
	fmt.Println("Type .help for commands. End SQL statements with ';'.")

	if err := s.loop(os.Stdin); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// loop reads input until EOF, buffering lines until a statement terminator
// (';') is seen, then dispatching the accumulated statement.
func (s *session) loop(in *os.File) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var buf strings.Builder
	s.prompt(false)

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// Dot-commands are only recognized at the start of a fresh statement.
		if buf.Len() == 0 && strings.HasPrefix(trimmed, ".") {
			if s.runDotCommand(trimmed) {
				return nil // .exit / .quit
			}
			s.prompt(false)
			continue
		}

		if buf.Len() > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(line)

		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(buf.String())
			stmt = strings.TrimSuffix(stmt, ";")
			buf.Reset()
			if stmt != "" {
				s.execute(stmt)
			}
			s.prompt(false)
			continue
		}
		s.prompt(true) // continuation prompt
	}
	fmt.Println()
	return scanner.Err()
}

// prompt prints the primary or continuation prompt.
func (s *session) prompt(continuation bool) {
	switch {
	case continuation:
		fmt.Print("  ...> ")
	case s.tx != nil:
		fmt.Print("minidb*> ") // asterisk signals an open transaction
	default:
		fmt.Print("minidb> ")
	}
}

// runDotCommand handles meta commands. It returns true if the shell should exit.
func (s *session) runDotCommand(cmd string) bool {
	switch strings.ToLower(strings.Fields(cmd)[0]) {
	case ".exit", ".quit":
		if s.tx != nil {
			fmt.Println("rolling back open transaction before exit")
			_ = s.tx.Rollback()
		}
		fmt.Println("bye")
		return true
	case ".help":
		printHelp()
	case ".begin":
		s.begin()
	case ".commit":
		s.commit()
	case ".rollback":
		s.rollback()
	default:
		fmt.Printf("unknown command %q — type .help\n", cmd)
	}
	return false
}

func (s *session) begin() {
	if s.tx != nil {
		fmt.Println("already in a transaction")
		return
	}
	tx, err := s.db.Begin()
	if err != nil {
		fmt.Printf("begin failed: %v\n", err)
		return
	}
	s.tx = tx
	fmt.Println("transaction started")
}

func (s *session) commit() {
	if s.tx == nil {
		fmt.Println("no active transaction")
		return
	}
	if err := s.tx.Commit(); err != nil {
		fmt.Printf("commit failed: %v\n", err)
	} else {
		fmt.Println("committed")
	}
	s.tx = nil
}

func (s *session) rollback() {
	if s.tx == nil {
		fmt.Println("no active transaction")
		return
	}
	if err := s.tx.Rollback(); err != nil {
		fmt.Printf("rollback failed: %v\n", err)
	} else {
		fmt.Println("rolled back")
	}
	s.tx = nil
}

// execute runs a single SQL statement, choosing Query vs Exec based on the
// leading keyword, and routing through the active transaction if one is open.
func (s *session) execute(stmt string) {
	if isQuery(stmt) {
		s.runQuery(stmt)
		return
	}
	s.runExec(stmt)
}

// isQuery reports whether a statement returns rows (SELECT-like).
func isQuery(stmt string) bool {
	fields := strings.Fields(stmt)
	if len(fields) == 0 {
		return false
	}
	return strings.EqualFold(fields[0], "select")
}

func (s *session) runExec(stmt string) {
	var res sql.Result
	var err error
	if s.tx != nil {
		res, err = s.tx.Exec(stmt)
	} else {
		res, err = s.db.Exec(stmt)
	}
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
	if n, aerr := res.RowsAffected(); aerr == nil {
		fmt.Printf("OK (%d row(s) affected)\n", n)
	} else {
		fmt.Println("OK")
	}
}

func (s *session) runQuery(stmt string) {
	var rows *sql.Rows
	var err error
	if s.tx != nil {
		rows, err = s.tx.Query(stmt)
	} else {
		rows, err = s.db.Query(stmt)
	}
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, strings.Join(cols, "\t"))
	fmt.Fprintln(w, underline(cols))

	// Scan each row into a generic slice of values.
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	count := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Printf("error scanning row: %v\n", err)
			return
		}
		cells := make([]string, len(cols))
		for i, v := range vals {
			cells[i] = format(v)
		}
		fmt.Fprintln(w, strings.Join(cells, "\t"))
		count++
	}
	if err := rows.Err(); err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
	w.Flush()
	fmt.Printf("(%d row(s))\n", count)
}

// format renders a scanned value for display.
func format(v any) string {
	switch t := v.(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

// underline builds a header separator the same shape as the column row.
func underline(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = strings.Repeat("-", len(c))
	}
	return strings.Join(parts, "\t")
}

func printHelp() {
	fmt.Println(`Commands:
  .help            show this help
  .begin           start an explicit transaction
  .commit          commit the current transaction
  .rollback        roll back the current transaction
  .exit, .quit     leave the shell (rolls back any open transaction)

SQL:
  End statements with ';'. Statements may span multiple lines.
  SELECT ... runs as a query and prints a result table; everything else
  (CREATE, INSERT, UPDATE, DELETE, ...) runs and reports rows affected.`)
}
