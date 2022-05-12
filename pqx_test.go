package pqx_test

import (
	"testing"

	"blake.io/pqx/pqxtest"
	_ "github.com/lib/pq"
)

func TestMain(m *testing.M) {
	pqxtest.TestMain(m, `CREATE table foo (id int)`)
}

func TestStart(t *testing.T) {
	db := pqxtest.OpenDB(t)
	_, err := db.Exec(`INSERT into foo values (1)`)
	if err != nil {
		t.Fatal(err)
	}
}
