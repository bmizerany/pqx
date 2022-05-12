package pqx

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"tailscale.com/logtail/backoff"
)

type Postgres struct {
	Dir string

	startOnce sync.Once
	cmd       *exec.Cmd
	err       error
	db        *sql.DB
	port      string
	shutdown  func() error
	logs      sync.Map
	mainlog   strings.Builder
}

const magicSep = " :PQX_MAGIC_SEP: "

type logLine struct {
	dbname string
	level  string
	msg    string
}

func parseLogLine(line string) (ll logLine, ok bool) {
	var hasMagicSep bool
	ll.dbname, ll.msg, hasMagicSep = strings.Cut(line, magicSep)
	if !hasMagicSep {
		return logLine{}, false
	}
	ll.level, _, _ = strings.Cut(ll.msg, ":")
	return ll, true
}

func (p *Postgres) logf(msg string, args ...any) {
	// TODO(bmizerany): wrap below debug log in flag to avoid commenting/uncommenting
	// log.Printf("DEBUG: "+msg, args...)

	ll, ok := parseLogLine(msg)
	if !ok {
		fmt.Fprintf(&p.mainlog, msg, args...)
		return
	}

	v, ok := p.logs.Load(ll.dbname)
	if !ok {
		fmt.Fprint(&p.mainlog, ll.msg)
		return
	}

	v.(func(string, ...any))("[postgres]: " + ll.msg)
}

func (p *Postgres) Start(ctx context.Context, logf func(string, ...any)) error {
	do := func() error {
		// TODO: capture logs and report per test when something goes
		// wrong; assuming the use of a an automatic t.Run with a known
		// test name will suffice (i.e. TestThing/initdb)
		// TODO(bmizerany): reuse data dir if exists
		dataDir, err := initdb(ctx, p.logf, p.Dir)
		if err != nil {
			return err
		}

		p.port = randomPort()
		p.cmd = exec.CommandContext(ctx, "postgres",
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
			"-c", "log_line_prefix=%d :PQX_MAGIC_SEP: ",
		)

		p.cmd.Stdout = &lineWriter{logf: p.logf}
		p.cmd.Stderr = &lineWriter{logf: p.logf}
		if err := p.cmd.Start(); err != nil {
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
			p.cmd.Process.Signal(syscall.SIGQUIT)
			if err := p.cmd.Wait(); err != nil {
				return err
			}
			return nil
		}

		p.Flush() // flush any interesting/helpful logs before we start pinging
		return pingUntilUp(ctx, p.logf, p.db)
	}
	p.startOnce.Do(func() {
		p.err = do()
	})

	if p.err != nil {
		logf("pqx: failed to start: %v", p.err)
		logf("pqx: postgres start logs:")
		p.writeMainLogs(logf)
	}
	return p.err
}

func (p *Postgres) Flush() {
	if p.cmd == nil {
		return
	}
	flushLogs(p.cmd)
}

func (p *Postgres) Shutdown() error {
	defer p.Flush()
	if p.shutdown != nil {
		return p.shutdown()
	}
	return nil
}

func (p *Postgres) writeMainLogs(logf func(string, ...any)) {
	logf(p.mainlog.String())
}

// Open creates a database for the schema, connects to it, and returns the
// *sql.DB. .. more words needed here.
func (p *Postgres) CreateDB(ctx context.Context, logf func(string, ...any), name, schema string) (db *sql.DB, cleanup func(), err error) {
	if err := p.Start(ctx, logf); err != nil {
		return nil, nil, err
	}

	name = strings.ToLower(name)
	dbname := fmt.Sprintf("%s_%s", name, randomString())

	p.logs.Store(dbname, logf)
	defer p.Flush()

	q := fmt.Sprintf("CREATE DATABASE %s", dbname)
	_, err = p.db.ExecContext(ctx, q)
	if err != nil {
		p.writeMainLogs(logf)
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

		// Loggers
		// don't write them to logf because logf to avoid races with
		// underlying implementations... make better words here.
		p.logs.Delete(dbname)
	}

	_, err = db.ExecContext(ctx, schema)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return db, cleanup, nil
}

// initdb creates a new postgres database using the initdb command and returns
// the directory it was created in, or an error if any.
func initdb(ctx context.Context, logf func(string, ...any), rootDir string) (dir string, err error) {
	dataDir, err := filepath.Abs(filepath.Join(rootDir, "data"))
	if err != nil {
		return "", err
	}
	if isPostgresDir(dataDir) {
		return dataDir, nil
	}

	cmd := exec.CommandContext(ctx, "initdb", dataDir)
	cmd.Stdout = &lineWriter{logf: logf}
	cmd.Stderr = &lineWriter{logf: logf}
	defer flushLogs(cmd)
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

type lineWriter struct {
	logf func(string, ...any)

	lineBuf strings.Builder
}

func (lw *lineWriter) Flush() error {
	if lw == nil {
		return nil
	}
	lw.logf(lw.lineBuf.String())
	lw.lineBuf.Reset()
	return nil
}

var newline = []byte{'\n'}

func (lw *lineWriter) Write(p []byte) (n int, err error) {
	p0 := p
	for {
		before, after, hasNewline := bytes.Cut(p, newline)
		lw.lineBuf.Write(before)
		if hasNewline {
			lw.lineBuf.WriteByte('\n')
			if err := lw.Flush(); err != nil {
				return 0, err
			}
			p = after
		} else {
			return len(p0), nil
		}
	}
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
	rand.Read(buf[:])
	return fmt.Sprintf("%x", buf)
}
