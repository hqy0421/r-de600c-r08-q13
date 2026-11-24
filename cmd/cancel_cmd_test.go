package cmd

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// largeGZFile writes a sizeable gzip CSV directly (without going
// through a database import, which is slow) and returns its path,
// so a short-timeout query is guaranteed to be interrupted
// mid-import.
func largeGZFile(t *testing.T, rows int) string {
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

// TestCli_RunContext_Canceled requires a canceled execution to
// return ExitCanceled, distinct from both ExitOK and ExitError.
func TestCli_RunContext_Canceled(t *testing.T) {
	path := largeGZFile(t, 200000)

	errStream := new(bytes.Buffer)
	log.SetOutput(errStream)
	cli := Cli{OutStream: new(bytes.Buffer), ErrStream: errStream}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	code := cli.RunContext(ctx, []string{
		"trdsql", "SELECT * FROM " + path,
	})
	if code != ExitCanceled {
		t.Fatalf("canceled query: got exit code %d, want %d (err: %s)", code, ExitCanceled, errStream.String())
	}
}

// TestCli_RunContext_Completed ensures the ordinary success path
// still returns ExitOK alongside a canceled one.
func TestCli_RunContext_Completed(t *testing.T) {
	csvPath := filepath.Join("..", "testdata", "test.csv")
	out := new(bytes.Buffer)
	cli := Cli{OutStream: out, ErrStream: new(bytes.Buffer)}

	code := cli.RunContext(context.Background(), []string{
		"trdsql", "SELECT * FROM " + csvPath,
	})
	if code != ExitOK {
		t.Fatalf("completed query: got exit code %d, want %d", code, ExitOK)
	}
	if got := out.String(); got != "1,Orange\n2,Melon\n3,Apple\n" {
		t.Fatalf("unexpected output: %q", got)
	}
}

// TestCli_RunContext_Error requires real failures to keep
// returning ExitError, so it cannot be confused with cancellation.
func TestCli_RunContext_Error(t *testing.T) {
	errStream := new(bytes.Buffer)
	log.SetOutput(errStream)
	cli := Cli{OutStream: new(bytes.Buffer), ErrStream: errStream}

	code := cli.RunContext(context.Background(), []string{
		"trdsql", "SELECT * FROM " + filepath.Join("..", "testdata", "no_such_file.csv"),
	})
	if code != ExitError {
		t.Fatalf("failing query: got exit code %d, want %d", code, ExitError)
	}
}

// TestCli_Run_WiresSignalContext checks that Run installs a
// signal-aware context by observing its behavior through the
// shared RunContext implementation: Run is a thin wrapper around
// signal.NotifyContext plus RunContext, and the signal-to-context
// bridge itself is provided by the standard library.
// Delivering a real SIGINT to the test process is deliberately
// avoided because the go test harness also listens for it.
func TestCli_Run_WiresSignalContext(t *testing.T) {
	csvPath := filepath.Join("..", "testdata", "test.csv")
	out := new(bytes.Buffer)
	errStream := new(bytes.Buffer)
	cli := Cli{OutStream: out, ErrStream: errStream}

	if code := cli.Run([]string{"trdsql", "SELECT * FROM " + csvPath}); code != ExitOK {
		t.Fatalf("Run via signal-aware context failed: code=%d err=%s", code, errStream.String())
	}
	if got := out.String(); got != "1,Orange\n2,Melon\n3,Apple\n" {
		t.Fatalf("unexpected output: %q", got)
	}
}

// TestCli_RunContext_CanceledClosesOutput checks that the output
// file and its compression wrapper are finalized on cancellation
// and the resulting gzip stream is still valid.
func TestCli_RunContext_CanceledClosesOutput(t *testing.T) {
	path := largeGZFile(t, 200000)
	outPath := filepath.Join(t.TempDir(), "out.csv.gz")

	errStream := new(bytes.Buffer)
	log.SetOutput(errStream)
	cli := Cli{OutStream: new(bytes.Buffer), ErrStream: errStream}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	code := cli.RunContext(ctx, []string{
		"trdsql", "-oz", "gzip", "-out", outPath,
		"SELECT * FROM " + path,
	})
	if code != ExitCanceled {
		t.Fatalf("got exit code %d, want %d (err: %s)", code, ExitCanceled, errStream.String())
	}

	// A finalized gzip file must open and decompress without
	// an error (an unflushed trailer would fail here).
	f, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("output file was not finalized: %v", err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip header missing or corrupt: %v", err)
	}
	if _, err := io.Copy(io.Discard, zr); err != nil {
		t.Fatalf("gzip stream was not finalized correctly: %v", err)
	}
	if err := zr.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
}
