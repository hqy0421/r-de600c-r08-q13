package trdsql

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// trackingReadCloser records whether Close was called,
// so the import read chain can be checked for leaks.
type trackingReadCloser struct {
	reader io.Reader
	mu     sync.Mutex
	closed bool
}

func (t *trackingReadCloser) Read(p []byte) (int, error) {
	return t.reader.Read(p)
}

func (t *trackingReadCloser) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return nil
}

func (t *trackingReadCloser) isClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// TestUncompressedReader_CloseChain makes sure that closing
// the reader returned for a compressed file also closes
// the underlying source, which is what lets a canceled query
// release its open buffered reader and file handle.
func TestUncompressedReader_CloseChain(t *testing.T) {
	t.Run("gzip", func(t *testing.T) {
		raw := new(bytes.Buffer)
		zw := gzip.NewWriter(raw)
		if _, err := zw.Write([]byte("c1,c2\n1,Orange\n2,Melon\n")); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}

		source := &trackingReadCloser{reader: bytes.NewReader(raw.Bytes())}
		rc := uncompressedReader(source)

		got, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if string(got) != "c1,c2\n1,Orange\n2,Melon\n" {
			t.Fatalf("unexpected decompressed content: %q", got)
		}

		if err := rc.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if !source.isClosed() {
			t.Fatal("underlying source was not closed with the decompressor")
		}
	})

	// Plain (uncompressed) input must still close the source.
	t.Run("plain", func(t *testing.T) {
		source := &trackingReadCloser{reader: strings.NewReader("c1,c2\n1,Orange\n")}
		rc := uncompressedReader(source)
		if _, err := io.ReadAll(rc); err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if !source.isClosed() {
			t.Fatal("underlying source was not closed")
		}
	})

	// Empty and shorter-than-magic sources must be closable too.
	t.Run("empty", func(t *testing.T) {
		source := &trackingReadCloser{reader: bytes.NewReader(nil)}
		rc := uncompressedReader(source)
		if _, err := io.ReadAll(rc); err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if !source.isClosed() {
			t.Fatal("underlying source was not closed")
		}
	})
}

// makeLargeGzipCSV writes a gzip-compressed CSV with the given
// number of data rows into a temporary file and returns its path.
func makeLargeGzipCSV(t *testing.T, rows int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "large.csv.gz")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw := gzip.NewWriter(file)
	w := bufio.NewWriter(zw)
	for i := range rows {
		if _, err := w.WriteString("row" + strconv.Itoa(i) + ",value\n"); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestExecContext_ParallelSameCompressedFile runs two queries
// against the same compressed raw file in parallel, cancels one
// of them, and requires that:
//   - the canceled query returns a context error distinguishable
//     with errors.Is(err, context.Canceled);
//   - the other query still builds its table and reads every row;
//   - neither query panics or races (run with -race).
func TestExecContext_ParallelSameCompressedFile(t *testing.T) {
	// Set the package-level multi-query flag once before the
	// parallel subtests: EnableMultipleQueries writes package
	// state and must not be called concurrently.
	EnableMultipleQueries()

	const rows = 200000
	path := makeLargeGzipCSV(t, rows)
	query := "SELECT * FROM " + path

	newTRD := func(out io.Writer) *TRDSQL {
		return NewTRDSQL(
			NewImporter(InFormat(GUESS)),
			NewExporter(NewWriter(OutStream(out))),
		)
	}

	t.Run("canceled", func(t *testing.T) {
		t.Parallel()
		out := new(bytes.Buffer)
		trd := newTRD(out)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		err := trd.ExecContext(ctx, query)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	})

	t.Run("completed", func(t *testing.T) {
		t.Parallel()
		out := new(bytes.Buffer)
		trd := newTRD(out)

		if err := trd.ExecContext(context.Background(), query); err != nil {
			t.Fatalf("the parallel query must still complete: %v", err)
		}
		lines := strings.Count(strings.TrimRight(out.String(), "\n"), "\n") + 1
		if lines != rows {
			t.Fatalf("expected %d output rows, got %d", rows, lines)
		}
	})
}

// TestExecContext_AlreadyCanceled checks that a context canceled
// before execution starts surfaces context.Canceled through the
// wrapped error chain rather than a generic failure.
func TestExecContext_AlreadyCanceled(t *testing.T) {
	path := makeLargeGzipCSV(t, 1000)
	out := new(bytes.Buffer)
	trd := setDefaultTRDSQL(out)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := trd.ExecContext(ctx, "SELECT * FROM "+path)
	if err == nil {
		t.Fatal("expected an error for a canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected errors.Is(err, context.Canceled), got %v", err)
	}
}

// gateReader returns one CSV record on the first Read (so the
// reader can derive column names during construction) and blocks
// on every subsequent Read until ctx is done.
type gateReader struct {
	ctx       context.Context
	firstOnce sync.Once
}

func newGateReader(ctx context.Context) *gateReader {
	return &gateReader{ctx: ctx}
}

func (g *gateReader) Read(p []byte) (int, error) {
	first := false
	g.firstOnce.Do(func() { first = true })
	if first {
		n := copy(p, []byte("c1,c2\n"))
		return n, nil
	}
	select {
	case <-g.ctx.Done():
		return 0, g.ctx.Err()
	}
}

// gateWriter blocks WriteRow until ctx is done, keeping the
// export loop deterministically inside the result output phase.
type gateWriter struct {
	ctx context.Context
}

func (g *gateWriter) PreWrite(_ []string, _ []string) error { return nil }

func (g *gateWriter) WriteRow(_ []any, _ []string) error {
	select {
	case <-g.ctx.Done():
		return g.ctx.Err()
	}
}

func (g *gateWriter) PostWrite() error { return nil }

// TestExecContext_CancelBlockedImport cancels an import that is
// blocked reading its source and asserts that context.Canceled is
// observable through the wrapped error chain. The buffered read
// for file-backed imports is covered separately by
// TestUncompressedReader_CloseChain and TestExecContext_CancelClosesOpenFiles.
func TestExecContext_CancelBlockedImport(t *testing.T) {
	readCtx, readCancel := context.WithCancel(context.Background())
	reader := newGateReader(readCtx)

	importer, err := NewBufferImporter("blocked", reader, InFormat(CSV))
	if err != nil {
		t.Fatal(err)
	}
	trd := NewTRDSQL(importer, NewExporter(NewWriter(OutStream(io.Discard))))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- trd.ExecContext(ctx, "SELECT * FROM blocked")
	}()

	// Give the import a moment to reach the blocked Read.
	time.Sleep(50 * time.Millisecond)
	cancel()
	readCancel()

	select {
	case execErr := <-done:
		if !errors.Is(execErr, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", execErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ExecContext did not return after cancellation")
	}
}

// TestExecContext_CancelBlockedExport cancels a query while it is
// blocked writing its result, proving the output phase honors
// cancellation and the error stays distinguishable from completion.
func TestExecContext_CancelBlockedExport(t *testing.T) {
	path := makeLargeGzipCSV(t, 1000)
	importer := NewImporter(InFormat(GUESS))
	blockCtx, blockCancel := context.WithCancel(context.Background())
	exporter := NewExporter(&gateWriter{ctx: blockCtx})
	trd := NewTRDSQL(importer, exporter)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- trd.ExecContext(ctx, "SELECT * FROM "+path)
	}()

	// Wait until the import has finished and the export is blocked.
	time.Sleep(100 * time.Millisecond)
	cancel()
	blockCancel()

	select {
	case execErr := <-done:
		if !errors.Is(execErr, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", execErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ExecContext did not return after cancellation")
	}
}

// TestExecContext_CancelClosesOpenFiles checks that a canceled
// import releases the compressed file's descriptor instead of
// leaking it (Linux /proc-based; skipped elsewhere).
func TestExecContext_CancelClosesOpenFiles(t *testing.T) {
	procSelfFD := "/proc/self/fd"
	if _, err := os.Stat(procSelfFD); err != nil {
		t.Skip("/proc/self/fd is not available")
	}
	path := makeLargeGzipCSV(t, 500000)

	countFDs := func() int {
		entries, err := os.ReadDir(procSelfFD)
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}

	before := countFDs()
	trd := setDefaultTRDSQL(io.Discard)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	err := trd.ExecContext(ctx, "SELECT * FROM "+path)
	cancel()
	if err == nil {
		t.Fatal("expected the canceled query to return an error")
	}

	// Run a couple of GC/settle rounds; descriptors are closed
	// synchronously in the deferred chain, so this should hold.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if countFDs() <= before {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("file descriptor leak: %d open before, %d after", before, countFDs())
}

// TestExecContext_CancelGlobImport cancels a wildcard import
// (backed by an io.Pipe fed from a goroutine) and requires the
// feeding goroutine to wind down instead of blocking forever.
func TestExecContext_CancelGlobImport(t *testing.T) {
	dir := t.TempDir()
	// Several compressed files matched by one glob pattern.
	for n := range 5 {
		path := filepath.Join(dir, "part"+strconv.Itoa(n)+".csv.gz")
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		zw := gzip.NewWriter(file)
		w := bufio.NewWriter(zw)
		for i := range 50000 {
			if _, err := w.WriteString("row" + strconv.Itoa(i) + ",value\n"); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}

	EnableMultipleQueries()
	trd := NewTRDSQL(
		NewImporter(InFormat(GUESS)),
		NewExporter(NewWriter(OutStream(io.Discard))),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- trd.ExecContext(ctx, "SELECT * FROM "+filepath.Join(dir, "part*.csv.gz"))
	}()

	select {
	case execErr := <-done:
		if !errors.Is(execErr, context.DeadlineExceeded) && !errors.Is(execErr, context.Canceled) {
			t.Fatalf("expected a context error, got %v", execErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("glob import did not wind down after cancellation")
	}
}

// TestExecContext_RollbackOnCancel ensures a canceled import does
// not leave a committed table behind: rerunning the same query
// afterwards must succeed without "table already exists" errors.
func TestExecContext_RollbackOnCancel(t *testing.T) {
	path := makeLargeGzipCSV(t, 100000)

	canceledOut := new(bytes.Buffer)
	canceledTRD := setDefaultTRDSQL(canceledOut)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := canceledTRD.ExecContext(ctx, "SELECT * FROM "+path)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected a context error, got %v", err)
	}

	okOut := new(bytes.Buffer)
	okTRD := setDefaultTRDSQL(okOut)
	if err := okTRD.Exec("SELECT * FROM " + path); err != nil {
		t.Fatalf("rerun after cancellation failed: %v", err)
	}
}
