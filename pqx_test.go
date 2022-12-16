package pqx_test

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"blake.io/pqx/pqxtest"
	_ "github.com/lib/pq"
)

func TestMain(m *testing.M) {
	if os.Getenv("TESTING_ORPHAN") != "" {
		flag.Parse()

		// pqxtest bases the data directory off of the working
		// directory, and we want to avoid conflicts with the parent
		// process's postgres, so jump to a different directory before
		// starting postgres with pqxtest as the child.
		dir, err := os.MkdirTemp("", "pqxtest")
		if err != nil {
			panic(err)
		}
		_ = os.Chdir(dir)

		os.Unsetenv("TESTING_ORPHAN")
		pqxtest.Start(5*time.Second, 0)

		// picked up by TestOrphan
		fmt.Printf("dsn: %q\n", pqxtest.DSN())

		go func() {
			panic("intentional panic")
		}()
		select {} // panic will kill us
	} else {
		pqxtest.TestMain(m)
	}
}

func TestStart(t *testing.T) {
	const schema = `CREATE TABLE foo (n int);`
	db := pqxtest.CreateDB(t, schema)
	_, err := db.Exec(`INSERT into foo values (1)`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSubTestNames(t *testing.T) {
	cases := []string{
		"a",
		"a b c",
		"a, b, c",
		"(a)",
		"(B)",
		"a-b-c",
		"*a*b*c",
		"a*b*c",
	}

	for _, name := range cases {
		t.Run(name, func(t *testing.T) { pqxtest.CreateDB(t, "") })
	}
}

func TestLongNames(t *testing.T) {
	prefix := strings.Repeat("a", 100)

	cases := []string{
		prefix + "a",
		prefix + "b",
	}

	for _, name := range cases {
		t.Run(name, func(t *testing.T) { pqxtest.CreateDB(t, "") })
	}
}

func TestOrphan(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=none")
	cmd.Env = append(os.Environ(), "TESTING_ORPHAN=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected error")
	}
	t.Logf("output: \n%s", out)
	if !strings.Contains(string(out), "intentional panic") {
		t.Fatalf("expected panic")
	}
	r := bytes.NewReader(out)
	dsn := scanString(t, r, "dsn: %q\n")
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for {
		if err := db.Ping(); err == nil {
			t.Logf("ping failed: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}

}

func TestParallel(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Run("r", func(t *testing.T) {
			t.Parallel()
			db := pqxtest.CreateDB(t, "")
			_, err := db.Exec(`SELECT 1`)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("wait", func(t *testing.T) {
		t.Logf("waiting")
	})
}

func scanString(t *testing.T, r io.Reader, format string) string {
	t.Helper()
	var s string
	_, err := fmt.Fscanf(r, format, &s)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
