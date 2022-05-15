package pqx

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"blake.io/pqx/internal/logplex"
	"tailscale.com/logtail/backoff"
)

const DefaultVersion = "14.2.0"

type Postgres struct {
	Version string // for a list of versions by OS, see: https://mvnrepository.com/artifact/io.zonky.test.postgres
	Dir     string

	startOnce sync.Once
	err       error
	db        *sql.DB
	port      string
	shutdown  func() error
	out       *logplex.Logplex
}

const magicSep = " :PQX_MAGIC_SEP: "

type logLine struct {
	dbname string
	level  string
	msg    string
	cont   bool
}

func parseLogLine(line string) (ll logLine, ok bool) {
	var hasMagicSep bool
	ll.dbname, ll.msg, hasMagicSep = strings.Cut(line, magicSep)
	if !hasMagicSep {
		cont := len(line) > 0 && unicode.IsSpace(rune(line[0]))
		if cont {
			return logLine{cont: true, msg: line}, true
		}
		return logLine{}, false
	}
	ll.level, _, _ = strings.Cut(ll.msg, ":")
	return ll, true
}

func (p *Postgres) version() string {
	if p.Version != "" {
		return p.Version
	}
	return DefaultVersion
}

func (p *Postgres) Start(ctx context.Context, logf func(string, ...any)) error {
	do := func() error {
		binDir, err := fetchBinary(ctx, p.version())
		if err != nil {
			return err
		}

		// TODO: capture logs and report per test when something goes
		// wrong; assuming the use of a an automatic t.Run with a known
		// test name will suffice (i.e. TestThing/initdb)
		// TODO(bmizerany): reuse data dir if exists
		dataDir, err := initdb(ctx, p.out, binDir, p.Dir)
		if err != nil {
			return err
		}

		p.port = randomPort()

		const magicSep = " ::pqx:: "

		p.out = &logplex.Logplex{
			Sink: &logfWriter{logf},
			Split: func(line []byte) (string, []byte) {
				key, msg, found := bytes.Cut(line, []byte(magicSep))
				if !found {
					return "", line
				}
				return string(key), msg
			},
		}

		cmd := exec.CommandContext(ctx, binDir+"/postgres",
			// env
			"-d", "2",
			"-D", dataDir,
			"-p", p.port,

			// resources
			"-c", "shared_buffers=12MB", // TODO(bmizerany): make configurable
			"-c", "fsync=off",
			"-c", "synchronous_commit=off",
			"-c", "full_page_writes=off",

			// logs
			"-c", "log_line_prefix=%d"+magicSep,
		)

		cmd.Stdout = p.out
		cmd.Stderr = p.out
		if err := cmd.Start(); err != nil {
			return err
		}
		defer p.Flush()

		db, err := sql.Open("postgres", p.connStr("postgres"))
		if err != nil {
			return err
		}
		p.db = db
		p.shutdown = func() error {
			db.Close()
			if err := cmd.Process.Signal(syscall.SIGQUIT); err != nil {
				return err
			}
			if err := cmd.Wait(); err != nil {
				return err
			}
			return nil
		}

		p.Flush() // flush any interesting/helpful logs before we start pinging
		return pingUntilUp(ctx, logfFromWriter(p.out), p.db)
	}
	p.startOnce.Do(func() {
		p.err = do()
	})

	return p.err
}

func (p *Postgres) Flush() {
	p.out.Flush()
}

func (p *Postgres) Shutdown() error {
	defer p.Flush()
	if p.shutdown != nil {
		return p.shutdown()
	}
	return nil
}

// Open creates a database for the schema, connects to it, and returns the
// *sql.DB. .. more words needed here.
func (p *Postgres) CreateDB(ctx context.Context, logf func(string, ...any), name, schema string) (db *sql.DB, cleanup func(), err error) {
	if err := p.Start(ctx, logf); err != nil {
		return nil, nil, err
	}

	name = cleanName(name)
	dbname := fmt.Sprintf("%s_%s", name, randomString())

	defer p.Flush()

	q := fmt.Sprintf("CREATE DATABASE %s", dbname)
	_, err = p.db.ExecContext(ctx, q)
	if err != nil {
		p.Flush()
		return nil, nil, err
	}

	db, err = sql.Open("postgres", p.connStr(dbname))
	if err != nil {
		return nil, nil, err
	}

	cleanup = func() {
		db.Close()

		// flush any logs we have on hand, we may not get them all, but
		// at this point we'll only miss sessions disconnecting, etc.
		// TODO(bmizerany): wait for sentinal log line before proceeding after Flush?
		p.Flush()
	}

	if schema != "" {
		_, err = db.ExecContext(ctx, schema)
		if err != nil {
			cleanup()
			return nil, nil, err
		}
	}
	return db, cleanup, nil
}

// initdb creates a new postgres database using the initdb command and returns
// the directory it was created in, or an error if any.
func initdb(ctx context.Context, out io.Writer, binDir, rootDir string) (dir string, err error) {
	dataDir, err := filepath.Abs(filepath.Join(rootDir, "data"))
	if err != nil {
		return "", err
	}
	if isPostgresDir(dataDir) {
		return dataDir, nil
	}

	cmd := exec.CommandContext(ctx, path.Join(binDir, "initdb"), dataDir)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return dataDir, nil
}

// isPostgresDir return true iif dir exists, is a directory, and contains the
// file PG_VERSION; otherwise false.
func isPostgresDir(dir string) bool {
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return false
	}
	_, err = os.Stat(filepath.Join(dir, "PG_VERSION"))
	return err == nil
}

func (p *Postgres) connStr(dbname string) string {
	return fmt.Sprintf("host=localhost port=%s dbname=%s sslmode=disable", p.port, dbname)
}

// pingUntilUp pings the database until it's up; or the provided context is
// canceled; whichever comes first.
func pingUntilUp(ctx context.Context, logf func(string, ...any), db *sql.DB) error {
	b := backoff.NewBackoff("ping", logf, 1*time.Second)
	for {
		err := db.PingContext(ctx)
		if err == nil {
			return nil
		}
		logf("pqx: ping failed; retrying: %v", err)
		b.BackOff(ctx, err)
	}
}

func randomPort() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	return strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
}

func flushLogs(cmd *exec.Cmd) {
	cmd.Stdout.(*lineWriter).Flush()
	cmd.Stderr.(*lineWriter).Flush()
}

func randomString() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", buf)
}

func cleanName(name string) string {
	rr := []rune(name)
	for i, r := range rr {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			rr[i] = '_'
		}
	}
	return strings.ToLower(string(rr))
}

func logfFromWriter(w io.Writer) func(string, ...any) {
	return func(format string, args ...any) {
		fmt.Fprintf(w, format, args...)
	}
}

type logfWriter struct {
	logf func(string, ...any)
}

func (w *logfWriter) Write(p []byte) (int, error) {
	w.logf(string(p))
	return len(p), nil
}
