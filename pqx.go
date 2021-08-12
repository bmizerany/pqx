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

// Flags
var (
	flagV = flag.Int("pqx.d", 0, "postgres debug level (0-5)")
)

var flagParseOnce sync.Once

var startSchema string

// SetSchema sets the schema used by all calls to Start. It is not safe to call
// across goroutines.
func SetSchema(s string) {
	startSchema = s
}

// Start returns a sql.DB initialized with the schema set in SetSchema.
func Start(t *testing.T) *sql.DB {
	return StartWithSchema(t, startSchema)
}

func StartWithSchema(t testing.TB, schema string) *sql.DB {
	t.Helper()
	_, db := StartWithSchemaInfo(t, schema)
	return db
}

func StartWithSchemaInfo(t testing.TB, schema string) (cs string, db *sql.DB) {
	t.Helper()

	flagParseOnce.Do(flag.Parse)

	// each call to TempDir returns a new dir, so save the first it gives
	// us and pass around.
	dataDir := t.TempDir()

	initDB(t, dataDir)

	port, err := freePort()
	if err != nil {
		t.Fatal("pqx:", err)
	}

	// iosolate logs from data by using new TempDir
	f, err := os.CreateTemp(t.TempDir(), "csvlog")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	ctx, cancel := context.WithCancel(context.Background())
	pg := exec.CommandContext(
		ctx,
		"postgres",

		// args
		"-d", strconv.Itoa(*flagV),
		"-p", port,
		"-D", dataDir,
		"-c", "lc_messages=en_US.UTF-8",

		// logging
		"-c", "log_destination=stderr,csvlog",
		"-c", "logging_collector=on",
		"-c", "log_directory="+filepath.Dir(f.Name()),
		"-c", "log_filename="+filepath.Base(f.Name()),
	)

	if *flagV > 0 {
		// TODO(bmizerany): fold into t.Log
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

	cs = fmt.Sprintf("port=%s dbname=postgres sslmode=disable", port)
	db, err = sql.Open("postgres", cs)
	if err != nil {
		t.Fatal("pqx:", err)
	}

	// attempt to connect within 3 seconds, otherwise fail
	ctx, cancel = context.WithTimeout(ctx, 3*time.Second)
	for {
		if err := db.PingContext(ctx); err != nil {
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

		_, err = db.Exec(schema)
		if err != nil {
			t.Fatalf("error building schema: %v", err)
		}
		return cs, db
	}
}

func freePort() (port string, err error) {
	// To be reviewed issue #341
	// nolint:gosec
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	defer l.Close() // we close below, but be paranoid

	addr := l.Addr().(*net.TCPAddr)
	l.Close()
	return strconv.Itoa(addr.Port), nil
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

// caller must hold w.m
func (w *logLineWriter) init() {
	if w.buf == nil {
		w.buf = new(bytes.Buffer)
	}
}

// caller must hold w.m
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
