package pqx_test

import (
	"testing"

	"blake.io/pqx/pqxtest"
	_ "github.com/lib/pq"
)

func TestMain(m *testing.M) {
	pqxtest.TestMain(m)
}

func TestStart(t *testing.T) {
	const schema = `CREATE TABLE foo (n int);`
	db := pqxtest.CreateDB(t, schema)
	_, err := db.Exec(`INSERT into foo values (1)`)
	if err != nil {
		t.Fatal(err)
	}
}
