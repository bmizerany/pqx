package logplex

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"

	"kr.dev/diff"
)

func testSplitter(line []byte) (key, msg []byte) {
	key, msg, hasSep := bytes.Cut(line, []byte("::"))
	if hasSep {
		return key, msg
	}
	return nil, line
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
		Split: testSplitter,
	}

	write := func(s string) {
		t.Helper()
		n, err := lp.Write([]byte(s))
		if err != nil {
			t.Error(err)
		}
		if n != len(s) {
			t.Errorf("wrote %d bytes, want %d", n, len(s))
		}
	}

	write("nothing\n") //nolint
	diff.Test(t, t.Errorf, d0.String(), "nothing\n")
	d0.Reset()

	lp.Watch("d1", &d1)
	lp.Watch("d2", &d2)

	// unknowns sent to Sink
	write("zero\n")
	write("d0::zero\n") // unknown prefix; sent to Drain

	// d1
	write("d1::one\n")
	write("d1::a")
	write("b\n")

	// d2
	write("d2::t")
	write("wo\n")

	// d3
	write("d3::three\n") // write d3 before Watch

	lp.Watch("d3", &d3) // late watch
	write("d3:")        // split seperator
	write(":3\n\tcontinuation\n")

	// detach d1 so it goes to Sink
	lp.Unwatch("d1")
	write("d1::detached\n")

	diff.Test(t, t.Errorf, d0.String(), "zero\nd0::zero\nd3::three\nd1::detached\n") // captures d3 logs until Watch("d3", ...)
	diff.Test(t, t.Errorf, d1.String(), "one\nab\n")
	diff.Test(t, t.Errorf, d2.String(), "two\n")
	diff.Test(t, t.Errorf, d3.String(), "3\n\tcontinuation\n")
}

// run with -race
func TestConcurrency(t *testing.T) {
	var (
		d0 strings.Builder
		d1 strings.Builder
		d2 strings.Builder
	)

	lp := &Logplex{
		Sink: &d0,
	}
	lp.Watch("d1", &d1)
	lp.Watch("d2", &d2)

	const seq = "abcdefghijklmnopqrstuvwxyz"
	var g sync.WaitGroup
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		i := i
		g.Add(1)
		go func() {
			defer g.Done()
			for _, c := range seq {
				fmt.Fprintf(lp, "d%d::%c\n", i, c)
			}
		}()
	}

	g.Wait()
}

func TestWriteAllocs(t *testing.T) {
	lp := &Logplex{
		Sink:  io.Discard,
		Split: testSplitter,
	}
	lp.Watch("a", io.Discard)

	line := []byte("a::b\n")
	got := testing.AllocsPerRun(100, func() {
		lp.Write(line) //nolint
	})

	if got > 0 {
		t.Errorf("got %f allocs, want 0", got)
	}
}

func BenchmarkWrite(b *testing.B) {
	b.ReportAllocs()

	lp := &Logplex{
		Sink:  io.Discard,
		Split: testSplitter,
	}
	lp.Watch("a", io.Discard)

	line := []byte("a::b\n")
	for i := 0; i < b.N; i++ {
		lp.Write(line) //nolint
	}
}
