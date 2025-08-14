// internal/server/middleware_logging.go
package server

import (
	"context"
	"log/slog"
	"math/rand"
	"net/http"
	"time"
)

const (
	ctxLoggerKey    ctxKey = "logger"
	ctxRequestIDKey ctxKey = "req_id"
)

type statusWriter struct {
	http.ResponseWriter
	status  int
	written int64
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
func (w *statusWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.written += int64(n)
	return n, err
}

func (s *Server) WithRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				reqID := r.Context().Value(ctxRequestIDKey)
				s.Logger.Error("panic", "req_id", reqID, "path", r.URL.Path, "err", rec)
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("<Error><Code>InternalError</Code><Message>panic</Message></Error>"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) WithRequestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := genReqID()
		ctx := WithRequestID(r.Context(), reqID)

		l := s.Logger.With(
			slog.String("req_id", reqID),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("remote", r.RemoteAddr),
		)
		ctx = context.WithValue(ctx, ctxLoggerKey, l)

		ww := &statusWriter{ResponseWriter: w, status: 200}
		start := time.Now()

		// полезно вернуть ID запроса клиенту
		ww.Header().Set("x-amz-request-id", reqID)

		next.ServeHTTP(ww, r.WithContext(ctx))

		l.Info("request",
			slog.Int("status", ww.status),
			slog.Duration("dur", time.Since(start)),
			slog.Int64("bytes", ww.written),
		)
	})
}

func genReqID() string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 16)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// helper: взять логгер из контекста
func loggerFrom(r *http.Request) *slog.Logger {
	if l, ok := r.Context().Value(ctxLoggerKey).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}
