package pqx

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"tailscale.com/logtail/backoff"
)

// has a version
// has a schema
// has sql.DBs
// has a log
// has log sinks
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
	tmplName  string
}

func (p *Postgres) Start(ctx context.Context) error {
	do := func() error {
		// TODO: capture logs and report per test when something goes
		// wrong; assuming the use of a an automatic t.Run with a known
		// test name will suffice (i.e. TestThing/initdb)
		logfTODO := log.Printf

		// TODO(bmizerany): reuse data dir if exists
		if err := initdb(ctx, logfTODO, p.Dir); err != nil {
			return err
		}

		p.port = randomPort()
		p.cmd = exec.CommandContext(ctx, "postgres",
			"-D", p.Dir,
			"-p", p.port,
		)

		p.cmd.Stdout = &lineWriter{logf: logfTODO}
		p.cmd.Stderr = &lineWriter{logf: logfTODO}
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
		if err := pingUntilUp(ctx, logfTODO, p.db); err != nil {
			return err
		}

		p.tmplName = fmt.Sprintf("pqxtemplate_%s", digestShort(p.Schema))

		_, err = p.db.ExecContext(ctx, "CREATE DATABASE "+p.tmplName)
		if err != nil {
			return err
		}

		tdb, err := sql.Open("postgres", p.connStr(p.tmplName))
		if err != nil {
			return err
		}
		defer tdb.Close()

		_, err = tdb.ExecContext(ctx, p.Schema)
		return err
	}
	p.startOnce.Do(func() {
		p.err = do()
	})
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

// Open creates a database for the schema, connects to it, and returns the
// *sql.DB. .. more words needed here.
func (p *Postgres) Create(ctx context.Context, name string) (*sql.DB, error) {
	if err := p.Start(ctx); err != nil {
		return nil, err
	}

	name = strings.ToLower(name)
	dbname := fmt.Sprintf("%s_%s", name, randomString())
	q := fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", dbname, p.tmplName)
	_, err := p.db.ExecContext(context.Background(), q)
	if err != nil {
		return nil, err
	}

	return sql.Open("postgres", p.connStr(dbname))
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
