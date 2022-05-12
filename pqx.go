package pqx

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"tailscale.com/logtail/backoff"
)

type Postgres struct {
	Dir    string
	Schema string

	// TODO(bmizerany): OpenTimeout
	// TODO(bmizerany): StartTimeout

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
		if err := initdb(ctx, p.logf, p.Dir); err != nil {
			return err
		}

		p.port = randomPort()
		p.cmd = exec.CommandContext(ctx, "postgres",
			// env
			"-d", "2",
			"-D", p.Dir,
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

// TODO: document when to call this (in TestMain?)
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
func (p *Postgres) Create(ctx context.Context, name string, logf func(string, ...any)) (*sql.DB, error) {
	if err := p.Start(ctx, logf); err != nil {
		return nil, err
	}

	name = strings.ToLower(name)
	dbname := fmt.Sprintf("%s_%s", name, randomString())

	p.logs.Store(dbname, logf)
	defer p.Flush()

	q := fmt.Sprintf("CREATE DATABASE %s", dbname)
	_, err := p.db.ExecContext(ctx, q)
	if err != nil {
		p.writeMainLogs(logf)
		return nil, err
	}

	db, err := sql.Open("postgres", p.connStr(dbname))
	if err != nil {
		return nil, err
	}

	_, err = db.ExecContext(ctx, p.Schema)
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// initdb creates a new postgres database using the initdb command and returns
// the directory it was created in, or an error if any.
func initdb(ctx context.Context, logf func(string, ...any), dir string) error {
	cmd := exec.CommandContext(ctx, "initdb", dir)
	cmd.Stdout = &lineWriter{logf: logf}
	cmd.Stderr = &lineWriter{logf: logf}
	return cmd.Run()
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

const hexDigit = "0123456789abcdef"

func digest(s string) string {
	// TODO(bmizerany): double check that this is correct.
	hash := sha256.New()
	io.WriteString(hash, s)
	bs := hash.Sum(nil)
	var buf []byte
	for _, b := range bs {
		buf = append(buf, hexDigit[b>>4], hexDigit[b&0xf])
	}
	return string(buf)
}

func digestShort(s string) string {
	dig := digest(s)
	return dig[:7]
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
