package agent

import (
	"io"
	"sync"
)

// ioMultiWriter объединяет несколько writers в один. Используется, когда
// лог пишется одновременно в файл и в консоль.
func ioMultiWriter(writers []any) io.Writer {
	ws := make([]io.Writer, 0, len(writers))
	for _, writer := range writers {
		if w, ok := writer.(io.Writer); ok {
			ws = append(ws, w)
		}
	}
	return &syncWriter{writers: ws}
}

// syncWriter пишет в несколько writers последовательно. Запись
// синхронизирована: одновременные вызовы не перемешивают строки между
// файлом и консолью.
type syncWriter struct {
	writers []io.Writer
	mu      sync.Mutex
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	written := len(p)
	var firstErr error
	for _, writer := range s.writers {
		n, err := writer.Write(p)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if n < written {
			written = n
		}
	}
	return written, firstErr
}
