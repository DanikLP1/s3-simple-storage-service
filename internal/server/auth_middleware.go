package server

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/DanikLP1/s3-storage-service/internal/auth"
	"github.com/DanikLP1/s3-storage-service/internal/db"
)

type credProvider struct{ db *db.DB }

func (c credProvider) LookupSecret(accessKeyID string) (string, error) {
	u, err := c.db.FindUserByAccessKey(accessKeyID)
	if err != nil {
		return "", err
	}
	return u.SecretAccessKey, nil
}

type ctxKey string

const ctxUserKey ctxKey = "auth.user.ID"

func (s *Server) AuthMiddleware(next http.Handler) http.Handler {
	allowNoSign := os.Getenv("ALLOW_INSECURE_NOSIGN") == "1"

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if allowNoSign && r.Header.Get("Authorization") == "" {
			next.ServeHTTP(w, r)
			return
		}

		res, err := auth.VerifySigV4(r, credProvider{s.db}, auth.VerifyOptions{
			MaxSkew:              15 * time.Minute,
			AllowUnsignedPayload: true,
			ExpectedService:      "s3",
		})
		if err != nil {
			writeS3Error(w, http.StatusForbidden, "SignatureDoesNotMatch", err.Error(), r.URL.Path, "")
			return
		}

		u, err := s.db.FindUserByAccessKey(res.AccessKeyID) // верни структуру с ID
		if err == nil {
			ctx := context.WithValue(r.Context(), ctxUserKey, u.ID)
			next.ServeHTTP(w, r.WithContext(ctx))
		}
	})
}

func getUserIDFromCtx(ctx context.Context) uint {
	if v := ctx.Value(ctxUserKey); v != nil {
		if id, ok := v.(uint); ok {
			return id
		}
	}
	return 0
}

func WrapWriteCheck(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wc := &writeCheckResponseWriter{ResponseWriter: w}
		next.ServeHTTP(wc, r)
	})
}
