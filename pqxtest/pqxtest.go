// Package pqxtest provides functions for testing with a live, lightweight, standalone
// Postgres instance optimized for fast setup/teardown and cleanup.
//
// Starting and creating a database:
//
//  func TestMain(m *testing.M) {
//  	pqxtest.TestMain(m)
//  }
//
//  func TestSomething(t *testing.T) {
//  	db := pqxtest.CreateDB(t, "CREATE TABLE foo (id INT)")
//  	// ... do something with db ...
//  	// NOTE: db will be closed automatically when t.Cleanup is called and the database will be dropped.
//  }
package pqxtest

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
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

// Flags
var (
	flagDebugLevel = flag.Int("pqxtest.d", 0, "postgres debug level (see `postgres -d`)")
)

var (
	sharedPG *pqx.Postgres
)

// TestMain is a convenience function for running tests with a live Postgres
// instance. It starts the Postgres instance before calling m.Run, and then
// calls Shutdown after.
//
// Users that need do more in their TestMain, can use it as a reference.
func TestMain(m *testing.M) {
	flag.Parse()
	Start(5*time.Second, *flagDebugLevel)
	defer Shutdown() //nolint
	code := m.Run()
	Shutdown()
	os.Exit(code)
}

// Start starts a Postgres instance. The version used is determined by the
// PQX_PG_VERSION environment variable if set, otherwise pqx.DefaultVersion is
// used.
//
// The Postgres instance is started in a temoporary directory named after the
// current working directory and reused across runs.
func Start(timeout time.Duration, debugLevel int) {
	sharedPG = &pqx.Postgres{
		Version:    os.Getenv("PQX_PG_VERSION"),
		Dir:        getSharedDir(),
		DebugLevel: debugLevel,
	}

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

// Shutdown shuts down the shared Postgres instance.
func Shutdown() {
	if sharedPG == nil {
		return
	}
	if err := sharedPG.Shutdown(); err != nil {
		log.Printf("error shutting down Postgres: %v", err)
	}
}

// CreateDB creates and returns a database using the shared Postgres instance.
// The database will automatically be cleaned up just before the test ends.
//
// All logs associated with the database will be written to t.Logf.
func CreateDB(t testing.TB, schema string) *sql.DB {
	t.Helper()
	if sharedPG == nil {
		t.Fatal("pqxtest.TestMain not called")
	}
	t.Cleanup(func() {
		sharedPG.Flush()
	})

	name := cleanName(t.Name())
	db, dsn, cleanup, err := sharedPG.CreateDB(context.Background(), t.Logf, name, schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)

	t.Logf("[pqx]: psql '%s'", dsn)

	return db
}

// BreakForPSQL blocks the current goroutine allowing the user to interact with
// the database(s) associated with t by prior calls to CreateDB.
//
// Example Usage:
//
//  func TestSomething(t *testing.T) {
//  	db := pqxtest.CreateDB(t, "CREATE TABLE foo (id INT)")
//	defer pqxtest.BreakForPSQL(t) // will run even in the face of t.Fatal/Fail.
//  	// ... do something with db ...
//  }
func BreakForPSQL(t testing.TB) {
	t.Helper()
	if !testing.Verbose() {
		// Calling this function without -v means users will not see
		// this message if we use t.Logf, and the tests will silently
		// hang, which isn't ideal, so we ensure the users sees we're
		// blocking.
		fmt.Fprintf(os.Stderr, "%s is blocking for SQL", t.Name())
	}
	t.Logf("blocking for SQL")
	select {}
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
