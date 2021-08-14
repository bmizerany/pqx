// Package pqx provides support for automated testing of packages that use
// Postgres.
//
// This package is a work in progress.
//
// To write a test using pqx, use the Start or StartExtra functions in the form:
//  func Test(t *testing.T) {
//  	db := pqx.Start(t)
//  	...
//  }
//
// Flags
//
// Flags are passed using the "go test" command like "go test -pqx.d=1".
//
//  -pqx.d level
//  	Log all postgres log output to t.Log. This means each test's postgres
//  	logs are logged to their respective testing.T/B. The value level is
//  	passed to postgres. See `postgres --help` for more information on the
//  	-D flag values. The default is to log nothing.
package pqx

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	schema strings.Builder
)

// Append appends sql to the global schema string returned by Schema. A
// trailing newline and semicolon are added automatically.
func Append(sql string) {
	schema.WriteString(sql)
	schema.WriteString("\n;\n")
}

// Schema returns the concatenation of all sql passed to all calls to Append.
func Schema() string {
	return schema.String()
}

// Flags
var (
	flagD = flag.Int("pqx.d", 0, "postgres debug level (0-5)")
)

var flagParseOnce sync.Once

// Start is equivalent to:
//  db, _ := StartExtra(t, Schema())
func Start(t testing.TB) *sql.DB {
	t.Helper()
	db, _ := StartExtra(t, Schema())
	return db
}

// StartExtra starts a fresh Postgres service with a fresh data directory and
// returns the connection string for connecting to the service, and a
// ready-to-use *sql.DB connected to the service using the connection string.
// Both the service and data directory are cleaned up upon exiting the test.
//
// If the pqx.d flag is provided with a value greater than zero, then all logs
// produced by the Postgres service are logged using t.Log.
//
// If a query error occurs, the query is logged using t.Error with a 💥 placed
// where the error occurred in the query.
//
// Any non-query errors are logged using t.Fatal.
func StartExtra(t testing.TB, schema string) (db *sql.DB, connStr string) {
	t.Helper()

	flagParseOnce.Do(flag.Parse)

	dataDir := t.TempDir()

	initDB(t, dataDir)

	f, err := os.CreateTemp(t.TempDir(), "csvlog")
	if err != nil {
		t.Fatal(err)
	}
	f.Close() // close so that postgres may take over

	port := freePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	pg := exec.CommandContext(
		ctx,
		"postgres",

		// args
		"-d", strconv.Itoa(*flagD),
		"-p", port,
		"-D", dataDir,
		"-c", "lc_messages=en_US.UTF-8",

		// logging
		"-c", "log_destination=stderr,csvlog",
		"-c", "logging_collector=on",
		"-c", "log_directory="+filepath.Dir(f.Name()),
		"-c", "log_filename="+filepath.Base(f.Name()),
	)

	if *flagD > 0 {
		pg.Stdout = &logLineWriter{t: t}
		pg.Stderr = &logLineWriter{t: t}
	}

	err = pg.Start()
	if err != nil {
		t.Fatal(err)
	}

	// Override cancel to use SIGQUIT so that postgres will shutdown
	// gracefully by bringing down all child workers. If the process has
	// not started; cancel will fallback to the original behavior defined
	// by exec.CommandContext.
	t.Cleanup(func() {
		t.Helper()

		defer cancel() // ensure we cancel no matter what
		defer maybeFlush(pg.Stdout)
		defer maybeFlush(pg.Stderr)

		if t.Failed() {
			// TODO(bmizerany): move logs here for review and name
			// based on t.Name() they can be found in
			// f.Name()

			// TODO(bmizerany): allow option to keep postgres
			// running so the programmer may connect via psql, or
			// move data dir to a location for easy review with
			// psql and print instructions on how to connect with
			// postgres and psql commands.
		}

		// postgres automatically appends .csv to the filename for
		// csvlog. The non-csv file is stderr.
		csvlog := f.Name() + ".csv"

		logQueryErrors(t, csvlog)

		if pg.Process != nil {
			// Strongly suggest that postgres shutdown gracefully
			// (i.e. clean up shared memory). The offical way to do
			// so is to send SIGQUIT.
			err := pg.Process.Signal(syscall.SIGQUIT)
			if err != nil {
				t.Fatal("pqx:", err)
			}
		} else {
			// We don't have a PID; shutdown whatever might be
			// running, forcefully.
			cancel()
		}
		if err := pg.Wait(); err != nil {
			t.Fatal("pqx:", err)
		}
	})

	connStr = fmt.Sprintf("port=%s dbname=postgres sslmode=disable", port)
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		t.Fatal("pqx:", err)
	}

	// attempt to connect within 3 seconds, otherwise fail
	ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
	for {
		if err := db.PingContext(ctx); err != nil {
			// TODO(bmizerany): Logging each ping error is noisy in the
			// common case because postgres needs some time to boot, but in
			// rare circumstances it might be useful to log the errors if
			// one is having trouble connecting/booting postgres. It might
			// be helpful to add a flag like `pqx.dd` for verbose logging?
			// Or maybe log the errors if the level set by pqx.d is
			// >0 because that signals verbosity is desired in
			// logs.

			// if the context was canceled, the ctx.Done() case will hit and
			// we'll fail; otherwise we ignore the error, sleep, and
			// then try again.
			select {
			case <-time.After(100 * time.Millisecond):
				continue
			case <-ctx.Done():
				t.Fatal("pqx: context canceled before postgres started:", ctx.Err())
			}
		}

		_, err := db.ExecContext(ctx, schema)
		if err != nil {
			t.Fatal(err)
		}

		return db, connStr
	}
}

func freePort(t testing.TB) string {
	// To be reviewed issue #341
	// nolint:gosec
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close() // we close below, but be paranoid

	addr := l.Addr().(*net.TCPAddr)
	l.Close()
	return strconv.Itoa(addr.Port)
}

func initDB(t testing.TB, dataPath string) {
	cmd := exec.Command("initdb", dataPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Log(string(out))
		t.Fatal("initdb:", err)
	}
}

// TODO(bmizerany): introduce periodic flushing much like httputil.ReverseProxy
type logLineWriter struct {
	t   testing.TB
	buf *bytes.Buffer
}

func (w *logLineWriter) Write(b []byte) (int, error) {
	w.init()
	defer w.maybeLogLine()

	n, err := w.buf.Write(b)
	if err != nil {
		return n, err
	}

	return n, nil
}

// caller must hold w.m
func (w *logLineWriter) maybeLogLine() (ok bool) {
	line, err := w.buf.ReadBytes('\n')
	if err != nil {
		return false
	}
	if len(line) > 0 {
		w.t.Logf("%s", line)
	}
	return true
}

func (w *logLineWriter) init() {
	if w.buf == nil {
		w.buf = new(bytes.Buffer)
	}
}

func (w *logLineWriter) Flush() {
	w.init()
	for {
		if w.maybeLogLine() {
			continue
		}
		if w.buf.Len() > 0 {
			w.t.Log(w.buf.String())
		}
		return
	}
}

func maybeFlush(w io.Writer) {
	f, _ := w.(interface{ Flush() })
	if f != nil {
		f.Flush()
	}
}

const (
	colLogTime = iota
	colUserName
	colDatabAseName
	colProcessID
	colConnectionFrom
	colSessionID
	colSessionLineNum
	colCommandTag
	colSessionStartTime
	colVirtualTransactionID
	colTransactionID
	colErrorSeverity
	colSqlStateCode
	colMessage
	colDetail
	colHint
	colInternalQuery
	colInternalQueryPos
	colContext
	colQuery
	colQueryPos
	colLocation
	colApplicationName
	colBackendType
)

func logQueryErrors(t testing.TB, fname string) {
	t.Helper()

	// errors should be rare here, so don't make this a t.Helper so that
	// users are brought right into the context of what is going wrong in
	// the rare event something does go wrong.

	f, err := os.Open(fname)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	cr := csv.NewReader(f)
	for {
		row, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}

		if row[colErrorSeverity] != "ERROR" {
			continue
		}

		q := []byte(row[colQuery])

		var offset int64
		if v := row[colQueryPos]; v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			offset = n
		}

		var sb strings.Builder
		if offset >= int64(len(q)) {
			sb.Write(q)
			sb.WriteString("💥")
		} else {
			for i, b := range q {
				if int64(i) == offset {
					sb.WriteString("💥")
				}
				sb.WriteByte(b)
			}
		}

		t.Logf("\n%s", sb.String())
	}
}
