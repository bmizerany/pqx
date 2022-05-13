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

func TestMain(m *testing.M) {
	defaultPG = &pqx.Postgres{
		Version: os.Getenv("PQX_PG_VERSION"),
		Dir:     getSharedDir(),
	}
	// TODO(bmizerany): timeout ctx?
	if err := defaultPG.Start(context.Background(), log.Printf); err != nil {
		log.Fatal(err)
	}
	defer defaultPG.Shutdown() //nolint
	code := m.Run()
	defaultPG.Shutdown() //nolint
	os.Exit(code)
}

func CreateDB(t *testing.T, schema string) *sql.DB {
	t.Helper()
	if defaultPG == nil {
		t.Fatal("pqxtest.TestMain not called")
	}

	db, cleanup, err := defaultPG.CreateDB(context.Background(), t.Logf, t.Name(), schema)
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
		Dir: getSharedDir(),
	}
	t.Cleanup(func() { p.Shutdown() }) //nolint

	db, cleanup, err := p.CreateDB(context.Background(), t.Logf, t.Name(), schema)
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
