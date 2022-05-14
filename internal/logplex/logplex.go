package logplex

import (
	"bytes"
	"io"
	"sync"
)

type Logplex struct {
	Sink io.Writer

	// if nil, all lines go to Drain
	Split func(line []byte) (key string, message []byte)

	lineBuf bytes.Buffer

	mu    sync.Mutex
	sinks map[string]io.Writer
}

func (lp *Logplex) Watch(prefix string, w io.Writer) {
	lp.mu.Lock()
	defer lp.mu.Unlock()
	if lp.sinks == nil {
		lp.sinks = map[string]io.Writer{}
	}
	lp.sinks[prefix] = w
}

var newline = []byte{'\n'}

// Write writes p to the underlying buffer and flushes each newline to their
// corresponding sinks.
func (lp *Logplex) Write(p []byte) (int, error) {
	p0 := p
	for {
		before, after, hasNewline := bytes.Cut(p, newline)
		lp.lineBuf.Write(before)
		if hasNewline {
			lp.lineBuf.Write(newline)
			if err := lp.Flush(); err != nil {
				return 0, err
			}
			p = after
		} else {
			return len(p0), nil
		}
	}
}

// Flush flushes the any underlying buffered contents to any corresponding sink.
//
// The contents flushed may not be a complete line, or have enough data to
// determine the proper sink and instead send to Sink.
//
// Flush should only be called when no more data will be written to Write.
func (lp *Logplex) Flush() error {
	defer lp.lineBuf.Reset()

	// There is only one line in the buffer. Check the prefix and send to
	// the appropriate sink.
	if lp.Split == nil {
		_, err := lp.Sink.Write(lp.lineBuf.Bytes())
		return err
	}

	line := lp.lineBuf.Bytes()
	key, message := lp.Split(line)
	for prefix, w := range lp.sinks {
		if key == prefix {
			_, err := w.Write(message)
			return err
		}
	}

	_, err := lp.Sink.Write(lp.lineBuf.Bytes())
	return err
}
