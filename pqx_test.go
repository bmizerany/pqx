package pqx_test

import (
	"testing"

	"blake.io/pqx"
	_ "github.com/lib/pq"
)

func init() {
	// TODO: pqx.SetVersion(`14`) // should only set for this packages tests
	// since package tests run in isolation from each other.
}

func TestStart(t *testing.T) {
	db := pqx.Start(t, `CREATE table foo (id int)`)
	_, err := db.Exec(`INSERT into foo values (1)`)
	if err != nil {
		t.Fatal(err)
	}
}
