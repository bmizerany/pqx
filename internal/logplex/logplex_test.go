package logplex

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"kr.dev/diff"
)

func TestPlex(t *testing.T) {
	var (
		d0 strings.Builder
		d1 strings.Builder
		d2 strings.Builder
	)

	p := &Logplex{
		Sink: &d0,
		Split: func(line []byte) (key string, msg []byte) {
			before, after, hasSep := bytes.Cut(line, []byte("::"))
			t.Logf("before: %q", before)
			t.Logf("after: %q", after)
			if hasSep {
				return string(before), after
			}
			return "", line
		},
	}
	p.Watch("d1", &d1)
	p.Watch("d2", &d2)

	io.WriteString(p, "zero\n")
	io.WriteString(p, "d0::zero\n") // unknown prefix; sent to Drain
	io.WriteString(p, "d1::one\n")
	io.WriteString(p, "d2::two\n")

	diff.Test(t, t.Errorf, d0.String(), "zero\nd0::zero\n")
	diff.Test(t, t.Errorf, d1.String(), "d1::one\n")
	diff.Test(t, t.Errorf, d2.String(), "d2::two\n")
}
