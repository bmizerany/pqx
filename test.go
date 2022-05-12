package pqx

import (
	"context"
	"database/sql"
	"testing"
)

// Open returns a sql.DB for schema if any. If no db has been started for
// schema, one will be started, connected to, and returned.
func Start(t *testing.T, schema string) *sql.DB {
	t.Helper()
	p := &Postgres{
		Schema: schema,
		Dir:    t.TempDir(),
	}
	t.Cleanup(func() { p.Shutdown() })

	db, err := p.Create(context.Background(), t.Name(), t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
