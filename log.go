package pqx

import (
	"bytes"
	"strings"
	"sync"
)

type lineWriter struct {
	logf func(string, ...any)

	mu      sync.Mutex
	lineBuf strings.Builder
}

func (lw *lineWriter) Flush() error {
	if lw == nil {
		return nil
	}

	lw.logf(lw.lineBuf.String())

	lw.mu.Lock()
	defer lw.mu.Unlock()
	lw.lineBuf.Reset()
	return nil
}

var newline = []byte{'\n'}

func (lw *lineWriter) writeLocked(p []byte, includeNewLine bool) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	lw.lineBuf.Write(p)
	if includeNewLine {
		lw.lineBuf.WriteByte('\n')
	}
}

func (lw *lineWriter) Write(p []byte) (n int, err error) {
	// log.Printf("DEBUG: %s", string(p))

	p0 := p
	for {
		before, after, hasNewline := bytes.Cut(p, newline)
		lw.writeLocked(before, hasNewline)
		if hasNewline {
			if err := lw.Flush(); err != nil {
				return 0, err
			}
			p = after
		} else {
			return len(p0), nil
		}
	}
}
