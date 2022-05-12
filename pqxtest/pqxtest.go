package pqxtest

import (
	"context"
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"testing"

	"blake.io/pqx"
)

var defaultPG *pqx.Postgres

func TestMain(m *testing.M, schema string) {
	defaultPG = &pqx.Postgres{
		Schema: schema,
		Dir:    getSharedDir(),
	}
	// TODO(bmizerany): timeout ctx?
	if err := defaultPG.Start(context.Background(), log.Printf); err != nil {
		log.Fatal(err)
	}
	defer defaultPG.Shutdown() // paranoid?
	code := m.Run()
	defaultPG.Shutdown()
	os.Exit(code)
}

func OpenDB(t *testing.T) *sql.DB {
	t.Helper()
	if defaultPG == nil {
		t.Fatal("pqxtest.TestMain not called")
	}

	db, cleanup, err := defaultPG.CreateDB(context.Background(), t.Name(), t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	return db
}

// Open returns a sql.DB for schema if any. If no db has been started for
// schema, one will be started, connected to, and returned.
func StartDB(t *testing.T, schema string) *sql.DB {
	t.Helper()

	p := &pqx.Postgres{
		Schema: schema,
		Dir:    getSharedDir(),
	}
	t.Cleanup(func() { p.Shutdown() })

	db, cleanup, err := p.CreateDB(context.Background(), t.Name(), t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)

	return db
}

func getSharedDir() string {
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return filepath.Join(os.TempDir(), "pqx", cwd)
}
