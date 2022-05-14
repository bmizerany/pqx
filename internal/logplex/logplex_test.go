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

func TestLogplex(t *testing.T) {
	var (
		d0 strings.Builder
		d1 strings.Builder
		d2 strings.Builder
		d3 strings.Builder
	)

	lp := &Logplex{
		Sink:  &d0,
		Split: newTestSplitter(),
	}

	lp.Write([]byte("nothing\n"))
	diff.Test(t, t.Errorf, d0.String(), "nothing\n")
	d0.Reset()

	lp.Watch("d1", &d1)
	lp.Watch("d2", &d2)

	lp.Write([]byte("zero\n"))
	lp.Write([]byte("d0::zero\n")) // unknown prefix; sent to Drain
	lp.Write([]byte("d1::one\n"))
	lp.Write([]byte("d1::a"))
	lp.Write([]byte("b\n"))
	lp.Write([]byte("d2::t"))
	lp.Write([]byte("wo\n"))

	lp.Write([]byte("d3::three\n")) // write d3 before Watch
	lp.Watch("d3", &d3)
	lp.Write([]byte("d3::3\n"))

	lp.Detach("d1")
	lp.Write([]byte("d1::detached\n"))

	diff.Test(t, t.Errorf, d0.String(), "zero\nd0::zero\nd3::three\nd1::detached\n") // captures d3 logs until Watch("d3", ...)
	diff.Test(t, t.Errorf, d1.String(), "one\nab\n")
	diff.Test(t, t.Errorf, d2.String(), "two\n")
	diff.Test(t, t.Errorf, d3.String(), "3\n")
}
