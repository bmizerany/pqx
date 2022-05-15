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
	"time"
	"unicode"

	"blake.io/pqx"
	"blake.io/pqx/internal/logplex"
)

var (
	testPG *pqx.Postgres
)

func TestMain(m *testing.M) {
	testPG = &pqx.Postgres{
		Version: os.Getenv("PQX_PG_VERSION"),
		Dir:     getSharedDir(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	startLog := new(bytes.Buffer)
	if err := testPG.Start(ctx, logplex.LogfFromWriter(startLog)); err != nil {
		if _, err := startLog.WriteTo(os.Stderr); err != nil {
			panic(err)
		}
		log.Fatal(err)
	}
	defer testPG.Shutdown() //nolint

	// free buffer
	startLog = nil

	code := m.Run()
	if err := testPG.Shutdown(); err != nil {
		log.Fatal(err)
	}
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
