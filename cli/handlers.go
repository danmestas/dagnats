package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/danmestas/dagnats/server"
	"github.com/danmestas/dagnats/worker"
)

const (
	handlerOutputMaxBytes = 10 << 20 // 10 MB
	execDefaultTimeout    = 5 * time.Minute
	httpDefaultTimeout    = 60 * time.Second
)

// limitWriter wraps a writer with a byte limit. Bytes beyond
// the limit are silently discarded. Returns total len(p) to
// prevent short-write errors from exec.Cmd.
type limitWriter struct {
	w       io.Writer
	limit   int64
	written int64
}

func (lw *limitWriter) Write(p []byte) (int, error) {
	total := len(p)
	if lw.written >= lw.limit {
		return total, nil
	}
	remaining := lw.limit - lw.written
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := lw.w.Write(p)
	lw.written += int64(n)
	if err != nil {
		return n, err
	}
	return total, nil
}

// buildHandler returns a HandlerFunc for the given WorkerConfig.
// Panics if config has neither exec nor http.
func buildHandler(
	cfg server.WorkerConfig,
) worker.HandlerFunc {
	if cfg.Task == "" {
		panic("buildHandler: task is empty")
	}
	if cfg.Exec != "" {
		return execHandler(cfg.Exec)
	}
	if cfg.HTTP != "" {
		method := cfg.HTTPMethod
		if method == "" {
			method = "POST"
		}
		return httpHandler(cfg.HTTP, method)
	}
	panic("buildHandler: no exec or http configured")
}

// execHandler returns a HandlerFunc that runs a shell command.
// Command string is split on spaces. Stdin receives task input.
// Stdout becomes output on success.
func execHandler(command string) worker.HandlerFunc {
	if command == "" {
		panic("execHandler: command is empty")
	}

	parts := strings.Fields(command)
	if len(parts) == 0 {
		panic("execHandler: command splits to zero parts")
	}

	return func(ctx worker.TaskContext) error {
		if ctx == nil {
			panic("execHandler: ctx is nil")
		}
		if len(parts) == 0 {
			panic("execHandler: parts is empty")
		}

		execCtx, cancel := context.WithTimeout(
			context.Background(), execDefaultTimeout,
		)
		defer cancel()

		cmd := exec.CommandContext(
			execCtx, parts[0], parts[1:]...,
		)
		cmd.Stdin = bytes.NewReader(ctx.Input())
		cmd.Env = append(cmd.Environ(),
			"DAGNATS_RUN_ID="+ctx.RunID(),
			"DAGNATS_STEP_ID="+ctx.StepID(),
			fmt.Sprintf(
				"DAGNATS_RETRY_COUNT=%d",
				ctx.RetryCount(),
			),
		)

		var stdout, stderr bytes.Buffer
		cmd.Stdout = &limitWriter{
			w: &stdout, limit: handlerOutputMaxBytes,
		}
		cmd.Stderr = &limitWriter{
			w: &stderr, limit: handlerOutputMaxBytes,
		}

		err := cmd.Run()
		if err != nil {
			var exitErr *exec.ExitError
			code := -1
			if errors.As(err, &exitErr) {
				code = exitErr.ExitCode()
			}
			errMsg := stderr.String()
			if errMsg == "" {
				errMsg = err.Error()
			}
			return ctx.Fail(fmt.Errorf(
				"exit %d: %s",
				code, strings.TrimSpace(errMsg),
			))
		}

		return ctx.Complete(stdout.Bytes())
	}
}

// httpHandler returns a HandlerFunc that sends task input to
// a URL. Response body becomes output on 2xx, error on non-2xx.
func httpHandler(
	url string, method string,
) worker.HandlerFunc {
	if url == "" {
		panic("httpHandler: url is empty")
	}
	if method == "" {
		panic("httpHandler: method is empty")
	}

	client := &http.Client{Timeout: httpDefaultTimeout}

	return func(ctx worker.TaskContext) error {
		if ctx == nil {
			panic("httpHandler: ctx is nil")
		}
		if url == "" {
			panic("httpHandler: url is empty in closure")
		}

		req, err := http.NewRequest(
			method, url, bytes.NewReader(ctx.Input()),
		)
		if err != nil {
			return ctx.Fail(fmt.Errorf(
				"create request: %v", err,
			))
		}
		req.Header.Set(
			"Content-Type", "application/json",
		)

		resp, err := client.Do(req)
		if err != nil {
			return ctx.Fail(fmt.Errorf(
				"http request: %v", err,
			))
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(io.LimitReader(
			resp.Body, handlerOutputMaxBytes,
		))
		if err != nil {
			return ctx.Fail(fmt.Errorf(
				"read response: %v", err,
			))
		}

		if resp.StatusCode >= 200 &&
			resp.StatusCode < 300 {
			return ctx.Complete(body)
		}

		return ctx.Fail(fmt.Errorf(
			"HTTP %d: %s",
			resp.StatusCode,
			strings.TrimSpace(string(body)),
		))
	}
}
