package logplex

import (
	"bytes"
	"strings"
	"testing"

	"kr.dev/diff"
)

func newTestSplitter() func(line []byte) (key string, msg []byte) {
	return func(line []byte) (key string, msg []byte) {
		before, after, hasSep := bytes.Cut(line, []byte("::"))
		if hasSep {
			return string(before), after
		}
		return "", line
	}
}

func TestPlex(t *testing.T) {
	var (
		d0 strings.Builder
		d1 strings.Builder
		d2 strings.Builder
		d3 strings.Builder
	)

	p := &Logplex{
		Sink:  &d0,
		Split: newTestSplitter(),
	}
	p.Watch("d1", &d1)
	p.Watch("d2", &d2)

	p.Write([]byte("zero\n"))
	p.Write([]byte("d0::zero\n")) // unknown prefix; sent to Drain
	p.Write([]byte("d1::one\n"))
	p.Write([]byte("d1::a"))
	p.Write([]byte("b\n"))
	p.Write([]byte("d2::t"))
	p.Write([]byte("wo\n"))
	p.Write([]byte("d3::three\n"))

	p.Watch("d3", &d3)
	p.Write([]byte("d3::3\n"))

	diff.Test(t, t.Errorf, d0.String(), "zero\nd0::zero\nd3::three\n") // captures d3 logs until Watch("d3", ...)
	diff.Test(t, t.Errorf, d1.String(), "one\nab\n")
	diff.Test(t, t.Errorf, d2.String(), "two\n")
	diff.Test(t, t.Errorf, d3.String(), "3\n")
}
