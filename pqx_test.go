package pqx

import (
	"context"
	"database/sql"
	"testing"
)

func TestIsolation(t *testing.T) {
	const schema = `
		CREATE TABLE test ( n int PRIMARY KEY )
	`

	db0 := Start(t, schema)
	db1 := Start(t, schema)

	ctx := context.Background()
	exec := func(db *sql.DB, q string) {
		t.Helper()
		_, err := db.ExecContext(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
	}

	exec(db0, "INSERT INTO test VALUES (1)")
	exec(db1, "INSERT INTO test VALUES (1)")
}
