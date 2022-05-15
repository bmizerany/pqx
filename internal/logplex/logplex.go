package logplex

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"unicode"
)

type Logplex struct {
	Sink io.Writer

	// if nil, all lines go to Drain
	Split func(line []byte) (key string, message []byte)

	lineBuf bytes.Buffer

	mu       sync.Mutex
	sinks    map[string]io.Writer
	lastSeen string
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
	lp.mu.Lock()
	defer lp.mu.Unlock()

	p0 := p
	for {
		before, after, hasNewline := bytes.Cut(p, newline)
		lp.lineBuf.Write(before)
		if hasNewline {
			lp.lineBuf.Write(newline)
			if err := lp.flushLocked(); err != nil {
				return 0, err
			}
			p = after
		} else {
			return len(p0), nil
		}
	}
}

// caller must hold mu
func (lp *Logplex) flushLocked() error {
	defer lp.lineBuf.Reset()

	// There is only one line in the buffer. Check the prefix and send to
	// the appropriate sink.
	if lp.Split == nil {
		_, err := lp.Sink.Write(lp.lineBuf.Bytes())
		return err
	}

	line := lp.lineBuf.Bytes()
	sent, err := lp.sendLine(line)
	if err != nil {
		return err
	}
	if sent {
		return nil
	}
	_, err = lp.Sink.Write(lp.lineBuf.Bytes())
	return err
}

func (lp *Logplex) Unwatch(prefix string) {
	lp.mu.Lock()
	defer lp.mu.Unlock()
	delete(lp.sinks, prefix)
}

// Flush flushes the any underlying buffered contents to any corresponding sink.
//
// The contents flushed may not be a complete line, or have enough data to
// determine the proper sink and instead send to Sink.
//
// Flush should only be called when no more data will be written to Write.
func (lp *Logplex) Flush() error {
	lp.mu.Lock()
	defer lp.mu.Unlock()
	return lp.flushLocked()
}

func (lp *Logplex) maybeWriteContinuation(line []byte) (sent bool, err error) {
	if isContinuation(line) {
		w := lp.sinks[lp.lastSeen]
		if w == nil {
			return false, nil
		}
		if _, err := w.Write(line); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// caller must hold mu
func (lp *Logplex) sendLine(line []byte) (sent bool, err error) {
	key, message := lp.Split(line)
	sent, err = lp.maybeWriteContinuation(message)
	if err != nil {
		return false, err
	}
	if sent {
		return true, nil
	}
	lp.lastSeen = key
	for prefix, w := range lp.sinks {
		if key == prefix {
			_, err := w.Write(message)
			return true, err
		}
	}
	return false, nil
}

type logfWriter struct {
	logf func(string, ...any)
}

func LogfWriter(logf func(string, ...any)) io.Writer {
	return &logfWriter{logf}
}

func (w *logfWriter) Write(p []byte) (int, error) {
	w.logf(string(p))
	return len(p), nil
}

func LogfFromWriter(w io.Writer) func(string, ...any) {
	return func(format string, args ...any) {
		fmt.Fprintf(w, format, args...)
	}
}

func isContinuation(line []byte) bool {
	return len(line) > 0 && unicode.IsSpace(rune(line[0]))
}
