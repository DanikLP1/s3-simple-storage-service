package server

import (
	"context"
	"net/http"
)

type ctxKeyRequestID struct{}

// WithRequestID кладёт requestID в context
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID{}, id)
}

// RequestIDFrom достаёт requestID из context или возвращает пустую строку
func RequestIDFrom(ctx context.Context) string {
	if v := ctx.Value(ctxKeyRequestID{}); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// requestIDFrom — удобный helper для http.Handler
func requestIDFrom(r *http.Request) string {
	return RequestIDFrom(r.Context())
}
