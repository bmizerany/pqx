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
	sharedPG *pqx.Postgres
)

func TestMain(m *testing.M) {
	Start(5 * time.Second)
	defer Shutdown() //nolint
	code := m.Run()
	Shutdown()
	os.Exit(code)
}

func Start(timeout time.Duration) {
	sharedPG = &pqx.Postgres{
		Version: os.Getenv("PQX_PG_VERSION"),
		Dir:     getSharedDir(),
	}

	// TODO: pre-fetch binary before start to maintain short timeout (take a fetchTimeout?)
	// TODO: move fetch to own package to make it easier to fetch here

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	startLog := new(bytes.Buffer)
	if err := sharedPG.Start(ctx, logplex.LogfFromWriter(startLog)); err != nil {
		if _, err := startLog.WriteTo(os.Stderr); err != nil {
			log.Fatalf("error writing start log: %v", err)
		}
		log.Fatalf("error starting Postgres: %v", err)
	}
}

func Shutdown() {
	if sharedPG == nil {
		return
	}
	if err := sharedPG.Shutdown(); err != nil {
		log.Printf("error shutting down Postgres: %v", err)
	}
}

func CreateDB(t *testing.T, schema string) *sql.DB {
	t.Helper()
	if sharedPG == nil {
		t.Fatal("pqxtest.TestMain not called")
	}
	t.Cleanup(func() {
		sharedPG.Flush()
	})

	name := cleanName(t.Name())
	db, cleanup, err := sharedPG.CreateDB(context.Background(), t.Logf, name, schema)
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
