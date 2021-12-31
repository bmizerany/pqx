// Package pqx provides support for automated testing of packages that use
// Postgres.
//
// This package is a work in progress.
//
// To write a test using pqx, use the Start or StartExtra functions in the form:
//  func Test(t *testing.T) {
//  	db := pqx.Start(t, "")
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
//
//  -pqx.c
//  	If set, -pqx.c will cause each test using Start or StartExtra to block
//  	just before cleaning up test data and print the psql command for
//  	connecting to the running test database. The test will resume upon
//  	receiving SIGINT (ctrl+c). This is useful for debugging tests.
//
//      NOTE: pqx uses t.Logf to print the psql command and `go test` will not
//      stream this unless the -test.v flag is set. To see the psql command
//      -pqx.c must be used with the -test.v flag.
//
//      Example:
//
//        go test -v -pqx.c -run=TestThatIsActingWeird
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
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Env
var (
	envD = os.Getenv("PQX_D")
)

// Flags
var (
	flagD = flag.Int("pqx.d", 0, "postgres debug level (0-5)")
	flagC = flag.Bool("pqx.c", false, "print psql command and block until SIGNINT for each test (see godoc for more information)")
)

var flagParseOnce sync.Once

// Start is equivalent to:
//  db, _ := StartExtra(t, Schema())
func Start(t testing.TB, initSQL string) *sql.DB {
	t.Helper()
	db, _ := StartExtra(t, initSQL)
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
func StartExtra(t testing.TB, initSQL string) (db *sql.DB, connStr string) {
	t.Helper()

	// TODO(bmizerany): test with `go test -count N` where N > 1 to ensure
	// we don't try to create a database that already exists.
	dbname := toDBName(t.Name())

	flagParseOnce.Do(flag.Parse)

	dataDir := t.TempDir()

	initDB(t, dataDir)

	f, err := os.CreateTemp(t.TempDir(), "csvlog")
	if err != nil {
		t.Fatal(err)
	}
	f.Close() // close so that postgres may take over

	port := freePort(t)
	if *flagD > 0 {
		envD = strconv.Itoa(*flagD)
	}
	if envD == "" {
		envD = "0"
	}

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

		if t.Failed() { //nolint
			// TODO(bmizerany): move logs here for review and name
			// based on t.Name() they can be found in
			// f.Name()
		}

		// TODO(bmizerany): cleanup new database in dbname

		// postgres automatically appends .csv to the filename for
		// csvlog. The non-csv file is stderr.
		csvlog := f.Name() + ".csv"

		logQueryErrors(t, csvlog, dbname)

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
	defer cancel()

	for {
		if err := db.PingContext(ctx); err != nil {
			if *flagD > 0 {
				t.Logf("pqx: error pinging %q: %v", connStr, err)
			}

			select {
			case <-time.After(100 * time.Millisecond):
				continue
			case <-ctx.Done():
				t.Fatal("pqx: context canceled before postgres started:", ctx.Err())
			}
		}

		// Until we have a cached "postgres" user connection setup by a
		// Main() we need to shutdown the postgres user connections to
		// keep things tidy and avoid wasting resources.
		defer db.Close()

		// TODO(bmizerany): override/replace ctx with new timeout?
		// right now this will cancel if the ping took a long time and
		// we just issued the command. In practice this may not be a
		// problem since most times we should be able to ping/connect
		// and create the database within the timeout.
		db, connStr := switchDB(ctx, t, db, connStr, dbname)

		_, err := db.ExecContext(ctx, initSQL)
		if err != nil {
			t.Fatal(err)
		}

		if *flagC {
			t.Cleanup(func() {
				t.Helper()

				t.Logf("pqx: pausing for introspection; CTRL+C (SIGINT) to resume")
				t.Logf("pqx: psql %q", connStr)

				waitForInterrupt()
			})
		}

		return db, connStr
	}
}

func waitForInterrupt() {
	ch := make(chan os.Signal, 1)
	defer signal.Stop(ch)
	signal.Notify(ch, os.Interrupt)
	<-ch
}

func switchDB(ctx context.Context, t testing.TB, db *sql.DB, connStr, dbname string) (*sql.DB, string) {
	q := "CREATE DATABASE " + dbname + ";" // no threat of sql-injection here, we own dbname.
	_, err := db.ExecContext(ctx, q)
	if err != nil {
		t.Fatal(err)
	}

	// Override the previous dbname by appending to the current connStr.
	// Drivers like lib/pq will handle this properly.
	connStr += " dbname=" + dbname

	if *flagD > 0 {
		t.Logf("pqx: switching to %q", connStr)
	}

	db, err = sql.Open("postgres", connStr)
	if err != nil {
		t.Fatal(err)
	}
	return db, connStr
}

func freePort(t testing.TB) string {
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
	defer w.Flush()

	n, err := w.buf.Write(b)
	if err != nil {
		return n, err
	}

	return n, nil
}

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

//nolint
const (
	colLogTime = iota
	colUserName
	colDatabaseName
	colProcessID
	colConnectionFrom
	colSessionID
	colSessionLineNum
	colCommandTag
	colSessionStartTime
	colVirtualTransactionID
	colTransactionID
	colErrorSeverity
	colSQLStateCode
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

func logQueryErrors(t testing.TB, fname, dbname string) {
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

		if row[colDatabaseName] != dbname {
			continue
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

func toDBName(name string) string {
	// TODO(bmizerany): probably a naive approach. Let's see how long it
	// takes until someone has a bug about this to decide what to do next.
	name = strings.Replace(name, "/", "_", -1)
	name = strings.Replace(name, "#", "_", -1)
	return strings.ToLower(name)
}
