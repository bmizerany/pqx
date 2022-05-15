package pqxtest

import (
	"bytes"
	"context"
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"

	"blake.io/pqx"
	"blake.io/pqx/internal/logplex"
)

var testPG *pqx.Postgres
var startLog bytes.Buffer

func TestMain(m *testing.M) {
	testPG = &pqx.Postgres{
		Version: os.Getenv("PQX_PG_VERSION"),
		Dir:     getSharedDir(),
	}
	// TODO(bmizerany): timeout ctx?
	if err := testPG.Start(context.Background(), logplex.LogfFromWriter(&startLog)); err != nil {
		if _, err := startLog.WriteTo(os.Stderr); err != nil {
			panic(err)
		}
		log.Fatal(err)
	}
	defer testPG.Shutdown() //nolint
	code := m.Run()
	testPG.Shutdown() //nolint
	os.Exit(code)
}

func CreateDB(t *testing.T, schema string) *sql.DB {
	t.Helper()
	if testPG == nil {
		t.Fatal("pqxtest.TestMain not called")
	}
	t.Cleanup(func() {
		testPG.Flush()
	})

	name := cleanName(t.Name())
	db, cleanup, err := testPG.CreateDB(context.Background(), t.Logf, name, schema)
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

func cleanName(name string) string {
	rr := []rune(name)
	for i, r := range rr {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			rr[i] = '_'
		}
	}
	return strings.ToLower(string(rr))
}
