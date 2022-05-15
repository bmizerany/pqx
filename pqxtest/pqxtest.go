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
	start := func() {
		sharedPG = &pqx.Postgres{
			Version: os.Getenv("PQX_PG_VERSION"),
			Dir:     getSharedDir(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		startLog := new(bytes.Buffer)
		if err := sharedPG.Start(ctx, logplex.LogfFromWriter(startLog)); err != nil {
			if _, err := startLog.WriteTo(os.Stderr); err != nil {
				panic(err)
			}
			log.Fatal(err)
		}
		// free buffer
		startLog = nil
	}

	start()
	defer sharedPG.Shutdown() //nolint

	code := m.Run()
	if err := sharedPG.Shutdown(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
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
