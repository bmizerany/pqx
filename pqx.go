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
	"sync"
	"syscall"
	"time"

	"blake.io/pqx/internal/fetch"
	"blake.io/pqx/internal/logplex"
	"golang.org/x/sync/errgroup"
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
	dropg     errgroup.Group
}

func (p *Postgres) version() string {
	if p.Version != "" {
		return p.Version
	}
	return DefaultVersion
}

// ctx only affects initdb and pingUntilUp; otherwise, the context is ignored.
func (p *Postgres) Start(ctx context.Context, logf func(string, ...any)) error {
	do := func() error {
		const magicSep = " ::pqx:: "

		p.out = &logplex.Logplex{
			Sink: logplex.LogfWriter(logf),
			Split: func(line []byte) (key, message []byte) {
				key, message, hasMagicSep := bytes.Cut(line, []byte(magicSep))
				if hasMagicSep {
					return key, message
				}
				return nil, line
			},
		}

		binDir, err := fetch.Binary(ctx, p.version())
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

		// run with disconnectext ctx so postgres continues running in background
		cmd := exec.CommandContext(context.Background(), binDir+"/postgres",
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
		return pingUntilUp(ctx, logf, p.db)
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
	if err := p.dropg.Wait(); err != nil {
		// always shutdown; but ignore error
		p.shutdown() //nolint
		return err
	}
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

	dbname := fmt.Sprintf("%s_%s", name, randomString())

	defer p.Flush()

	p.out.Watch(dbname, logplex.LogfWriter(logf))

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
		p.dropDB(ctx, dbname)

		// flush any logs we have on hand, we may not get them all, but
		// at this point we'll only miss sessions disconnecting, etc.
		// TODO(bmizerany): wait for sentinal log line before proceeding after Flush?
		p.Flush()
		p.out.Unwatch(dbname)
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

func (p *Postgres) dropDB(ctx context.Context, name string) {
	p.dropg.Go(func() error {
		_, err := p.db.ExecContext(ctx, "DROP DATABASE "+name)
		return err
	})
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

func randomString() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", buf)
}
