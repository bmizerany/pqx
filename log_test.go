package pqx

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	_ "github.com/lib/pq"
)

func TestHighlightErrorPosition(t *testing.T) {
	// NOTE: These tests will fail if -pqx.d is set greater than zero
	// because the ping logs will interfer. This is considered an
	// acceptable tradeoff for having ping logs when needed. This should be
	// the only test in existance it affects.

	tests := map[string]struct {
		q    string
		want string
	}{
		"beginning of query": {
			q:    "& SELECT",
			want: "\n&💥 SELECT\n",
		},
		"end of query": {
			q:    "SELECT &",
			want: "\nSELECT &💥\n",
		},
		"middle of query": {
			q:    "SELECT &, 1",
			want: "\nSELECT &,💥 1\n",
		},
		"middle of query (multiline)": {
			q:    "SELECT &,\n1",
			want: "\nSELECT &,💥\n1\n",
		},
	}

	tlc := &testLogCapture{TB: t}

	var want strings.Builder
	// TODO(bmizerany): better comment
	// we must hookup the check first because we have to allow the Cleanup
	// registered in StartExtra to fire first so that it flushes the
	// error log so we can see it.
	t.Cleanup(func() {
		if diff := cmp.Diff(want.String(), tlc.Got()); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
		t.Log(tlc.Got())
	})

	db := Start(tlc, ``)

	for _, tt := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		_, err := db.ExecContext(ctx, tt.q)
		if err == nil {
			t.Errorf("unexpected nil error")
		}

		want.WriteString(tt.want)

		// Checks are above in Cleanup and run just before the test
		// returns.
	}

}

type testLogCapture struct {
	testing.TB

	log strings.Builder
}

func (t *testLogCapture) Logf(format string, args ...interface{}) {
	fmt.Fprintln(&t.log, fmt.Sprintf(format, args...))
}

func (t *testLogCapture) Log(args ...interface{}) {
	fmt.Fprintln(&t.log, args...)
}

// Got returns the current log after and clears it for the next use.
// It is not safe to use across goroutines.
func (t *testLogCapture) Got() string {
	return t.log.String()
}
