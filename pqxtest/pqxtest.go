// Package pqxtest provides functions for testing with an embedded live,
// lightweight, standalone Postgres instance optimized for fast setup/teardown
// and cleanup.
//
// Starting, creating a database, using the database, and shutting down can all
// be done with:
//
//	func TestMain(m *testing.M) {
//		pqxtest.TestMain(m)
//	}
//
//	func TestSomething(t *testing.T) {
//		db := pqxtest.CreateDB(t, "CREATE TABLE foo (id INT)")
//		// ... do something with db ...
//		// NOTE: db will be closed automatically when t.Cleanup is called and the database will be dropped.
//	}
//
// # Developer Speed
//
// Pqxtest enables a developer to go from zero to well tested, production ready
// code that interacts with a database in no time. This means removing much of
// the toil associated with configuring and running a standalone postgres
// instance, the can be brittle is left standing across test runs,
// over engineering with abundant interfaces for mocking.
//
// # Easy CI
//
// Because pqxtest uses pqx which "embeds" a postgres into you application, CI
// is just "go test". No extra environment setup required. See this packages
// .github/workflows for an example.
//
// # No Mocks
//
// Mocking database interactions is dangerous. It is easy to create a test that
// gives false positives, or can cause developers precious time hunting down a
// bug that doesn't exist because they're getting false negatives from the mocks
// (i.e. the code would work if running against an actual database).
//
// Writing mocks and mocking code is almost always avoidable when interacting
// with a live database. Try pqxtest and ditch you mocking libraries and all
// the for-tests-only interfaces and use concrete types instead!
//
// # No global transaction
//
// Some test libraries create a single transaction for a test and return a
// wrapper-driver *sql.DB that runs all queries in a test in that transaction.
// This is less than ideal because, like mocks, this isn't how applications work
// in production, so  it can lead to weird behaviors and false positives. It
// also means you can't use those nifty driver specific functions you love.
// Pqxtest returns a real *sql.DB.
//
// # Speed
//
// Pqx is fast. It is designed to give you all the benefits of writing tests
// against a real database without slowing you down. That means no waiting for
// setup and teardown after the first test run. The first "go test" takes only a
// second or two to cache the standalone postgres binary and initialize the data
// directory, but subsequent runs skip these steps, making them very fast!
//
// # Logs
//
// Databases are created using the test name, and all logs associated with the
// test database are logged directly to the test t.Logf function. This provides
// rapid feedback and context when things go wrong. No more greping around in a
// shared log file.
//
// For example, the following failing tests will log the postgres logs for their
// databases only:
//
//	func TestFailingInsert(t *testing.T) {
//		db := pqxtest.CreateDB(t, ``)
//		_, err := db.Exec(`INSERT INTO foo VALUES (1)`)
//		if err != nil {
//			t.Fatal(err)
//		}
//	}
//
//	func TestFailingSelect(t *testing.T) {
//		db := pqxtest.CreateDB(t, ``)
//		_, err := db.Exec(`SELECT * FROM bar`)
//		if err != nil {
//			t.Fatal(err)
//		}
//	}
//
// Running ("go test") will produce:
//
//	--- FAIL: TestFailingInsert (0.03s)
//	    example_test.go:54: [pqx]: psql 'host=localhost port=51718 dbname=testfailinginsert_37d0bb55e5c8cb86 sslmode=disable'
//	    logplex.go:123: ERROR:  relation "foo" does not exist at character 13
//	    logplex.go:123: STATEMENT:  INSERT INTO foo VALUES (1)
//	    example_test.go:57: pq: relation "foo" does not exist
//	--- FAIL: TestFailingSelect (0.03s)
//	    example_test.go:62: [pqx]: psql 'host=localhost port=51718 dbname=testfailingselect_2649dda9b27d8c74 sslmode=disable'
//	    logplex.go:123: ERROR:  relation "bar" does not exist at character 15
//	    logplex.go:123: STATEMENT:  SELECT * FROM bar
//	    example_test.go:65: pq: relation "bar" does not exist
//
// Tip: Try running these tests with "go test -v -pqxtest.d=2" to see more detailed logs in the
// tests, or set it to 3 and see even more verbose logs.
//
// # PSQL
//
// Test databases can be accessed using the psql command line tool before they
// exist. Use the BlockForPSQL function to accomplish this.
//
// # No Config
//
// Pqx starts each postgres with reasonable defaults for the most common use
// cases. The defaults can be overridden by setting environment variables and
// using flags. See below.
//
// # Environment Variables
//
// The following environment variables are recognized:
//
//	PQX_PG_VERSION: Specifies the version of postgres to use. The default is pqx.DefaultVersion.
//
// # Flags
//
// pqxtest recognizes the following flag:
//
//	-pqxtest.d=<level>: Sets the debug level for the Postgres instance. See Logs for more details.
//
// Flags may be specified with go test like:
//
//	go test -v -pqxtest.d=2
package pqxtest

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
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

	dmu  sync.Mutex
	dsns = map[testing.TB][]string{}
)

// DSN returns the main dsn for the running postgres instance. It must only be
// call after a call to Start.
func DSN() string {
	return sharedPG.DSN("postgres")
}

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
// The Postgres instance is started in a temporary directory named after the
// current working directory and reused across runs.
func Start(timeout time.Duration, debugLevel int) {
	maybeBecomeSupervisor()

	sharedPG = &pqx.Postgres{
		Version:    os.Getenv("PQX_PG_VERSION"),
		Dir:        getSharedDir(),
		DebugLevel: debugLevel,
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	startLog := new(lockedBuffer)
	if err := sharedPG.Start(ctx, logplex.LogfFromWriter(startLog)); err != nil {
		if _, err := startLog.WriteTo(os.Stderr); err != nil {
			log.Fatalf("error writing start log: %v", err)
		}
		log.Fatalf("error starting Postgres: %v", err)
	}

	shutThisDownAfterMyDeath(sharedPG.Pid())
}

// Shutdown shuts down the shared Postgres instance.
func Shutdown() {
	if sharedPG == nil {
		return
	}
	if err := sharedPG.ShutdownAlone(); err != nil {
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
	name = fmt.Sprintf("%s_%s", name, randomString())
	db, dsn, cleanup, err := sharedPG.CreateDB(context.Background(), t.Logf, name, schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup()
		dmu.Lock()
		delete(dsns, t)
		dmu.Unlock()
	})

	dmu.Lock()
	dsns[t] = append(dsns[t], dsn)
	dmu.Unlock()

	return db
}

// BlockForPSQL logs the psql commands for connecting to all databases created
// by CreateDB in a test, and blocks the current goroutine allowing the user to
// interact with the databases.
//
// As a special case, if testing.Verbose is false, it logs to stderr to avoid
// silently hanging the tests.
//
// BreakForPSQL is intended for debugging only and not to be left in tests.
//
// Example Usage:
//
//	 func TestSomething(t *testing.T) {
//	 	db := pqxtest.CreateDB(t, "CREATE TABLE foo (id INT)")
//		defer pqxtest.BlockForPSQL(t) // will run even in the face of t.Fatal/Fail.
//	 	// ... do something with db ...
//	 }
func BlockForPSQL(t testing.TB) {
	t.Helper()

	logf := t.Logf
	if !testing.Verbose() {
		// Calling this function without -v means users will not see
		// this message if we use t.Logf, and the tests will silently
		// hang, which isn't ideal, so we ensure the users sees we're
		// blocking.
		logf = func(format string, args ...any) {
			t.Helper()
			fmt.Fprintf(os.Stderr, format+"\n", args...)
		}
	}

	dmu.Lock()
	dsns := dsns[t]
	dmu.Unlock()

	if len(dsns) == 0 {
		logf("[pqx]: BlockForPSQL: no databases to interact with")
	}

	for _, dsn := range dsns {
		logf("blocking for SQL; press Ctrl-C to quit")
		logf("[pqx]: psql '%s'", dsn)
	}
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

func randomString() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", buf)
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) WriteTo(w io.Writer) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.WriteTo(w)
}

func maybeBecomeSupervisor() {
	pid, _ := strconv.Atoi(os.Getenv("_PQX_SUP_PID"))
	if pid == 0 {
		return
	}
	log.SetFlags(0)
	awaitParentDeath()
	p, err := os.FindProcess(pid)
	if err != nil {
		log.Fatalf("find process: %v", err)
	}
	if err := p.Signal(syscall.Signal(syscall.SIGQUIT)); err != nil {
		log.Fatalf("error signaling process: %v", err)
	}
	os.Exit(0)
}

func awaitParentDeath() {
	_, _ = os.Stdin.Read(make([]byte, 1))
}

func shutThisDownAfterMyDeath(pid int) {
	exe, err := os.Executable()
	if err != nil {
		panic(err)
	}
	sup := exec.Command(exe)
	sup.Env = append(os.Environ(), "_PQX_SUP_PID="+strconv.Itoa(pid))
	sup.Stdout = os.Stdout
	sup.Stderr = os.Stderr

	// set a pipe we never write to as to block the supervisor until we die
	_, err = sup.StdinPipe()
	if err != nil {
		panic(err)
	}
	err = sup.Start()
	if err != nil {
		panic(err)
	}

	go func() {
		// Exiting this function without this reference to sup means
		// sup can become eliglble for GC after This exists. This means
		// the stdin pipe will also be collected, utlimately causing
		// sup to think it's parent has died, and it will shutdown
		// postgres and exit.
		_ = sup.Wait()
	}()
}
