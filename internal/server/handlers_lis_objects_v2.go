package server

import (
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/DanikLP1/s3-storage-service/internal/db"
)

func (s *Server) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	log := loggerFrom(r).With(slog.String("bucket", bucket))
	q := r.URL.Query()

	ct := q.Get("continuation-token")
	startAfter := q.Get("start-after")
	if ct != "" {
		// токен главнее start-after
		startAfter = ""
		// быстрая валидация токена (прежде чем идти в repo)
		if _, err := base64.RawURLEncoding.DecodeString(ct); err != nil {
			log.Warn("list_objects_v2.invalid_continuation_token")
			writeS3Error(w, http.StatusBadRequest, "InvalidArgument", "The continuation token provided is invalid.", r.URL.Path, requestIDFrom(r))
			return
		}
	}

	// delimiter — ровно один символ (как в AWS)
	delim := q.Get("delimiter")
	if len(delim) > 1 {
		log.Warn("list_objects_v2.invalid_delimiter", "delimiter", delim)
		writeS3Error(w, http.StatusBadRequest, "InvalidArgument", "delimiter must be a single character", r.URL.Path, requestIDFrom(r))
		return
	}

	log.Info("list_objects_v2.start",
		"prefix", q.Get("prefix"),
		"delimiter", delim,
		"max_keys", q.Get("max-keys"),
		"start_after", startAfter,
		"continuation_token", truncateForLog(ct),
	)

	ownerID := getUserIDFromCtx(r.Context())

	// 1) bucket lookup
	bucketID, err := s.db.BucketIDByName(bucket, ownerID)
	switch {
	case errors.Is(err, db.ErrNotFound):
		log.Warn("list_objects_v2.no_such_bucket")
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", "/"+bucket, requestIDFrom(r))
		return
	case err != nil:
		log.Error("list_objects_v2.db_fail_lookup", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", "/"+bucket, requestIDFrom(r))
		return
	}

	// 2) params
	maxKeys := 1000
	if v := q.Get("max-keys"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			maxKeys = n
		}
	}
	params := db.ListV2Params{
		BucketID:     bucketID,
		Prefix:       q.Get("prefix"),
		Delimiter:    delim,
		MaxKeys:      maxKeys,
		StartAfter:   startAfter, // уже с учётом игнора при токене
		ContTokenRaw: ct,         // передаём токен в repo
		FetchOwner:   q.Get("fetch-owner") == "true",
		EncodingType: q.Get("encoding-type"),
	}

	// 3) repo
	res, err := s.db.ListObjectsV2(r.Context(), params)
	if err != nil {
		if errors.Is(err, db.ErrInvalidContToken) {
			log.Warn("list_objects_v2.invalid_continuation_token_repo")
			writeS3Error(w, http.StatusBadRequest, "InvalidArgument", "The continuation token provided is invalid.", r.URL.Path, requestIDFrom(r))
			return
		}
		log.Error("list_objects_v2.db_fail_list", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", "/"+bucket, requestIDFrom(r))
		return
	}

	// 4) ответ
	xmlRes := toListV2XML(bucket, params, res)

	// x-amz-request-id для удобства отладки
	w.Header().Set("x-amz-request-id", requestIDFrom(r))
	writeListObjectsV2(w, xmlRes)

	log.Info("list_objects_v2.ok",
		"key_count", res.KeyCount,
		"is_truncated", res.IsTruncated,
		"next_token", truncateForLog(res.NextToken),
	)
}

const timeRFC3339 = "2006-01-02T15:04:05Z"

func toListV2XML(bucket string, p db.ListV2Params, res *db.ListV2Result) ListBucketResultV2 {
	out := ListBucketResultV2{
		Name:                  bucket,
		Prefix:                p.Prefix,
		Delimiter:             p.Delimiter,
		MaxKeys:               p.MaxKeys,
		EncodingType:          p.EncodingType,
		IsTruncated:           res.IsTruncated,
		KeyCount:              res.KeyCount,
		ContinuationToken:     p.ContTokenRaw,
		NextContinuationToken: res.NextToken,
		StartAfter:            p.StartAfter,
	}
	for _, cp := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, CommonPrefix{Prefix: cp})
	}
	for _, it := range res.Objects {
		obj := ListV2ObjectXML{
			Key:          it.Key,
			LastModified: it.LastModified.UTC().Format(timeRFC3339),
			Size:         it.Size,
		}
		if it.ETag != nil && *it.ETag != "" {
			obj.ETag = `"` + *it.ETag + `"`
		}
		if p.FetchOwner && it.OwnerID != nil {
			obj.Owner = &ListV2OwnerXML{ID: *it.OwnerID, DisplayName: coalesce(it.OwnerName, "")}
		}
		out.Contents = append(out.Contents, obj)
	}
	return out
}

func coalesce[T any](p *T, def T) T {
	if p != nil {
		return *p
	}
	return def
}

func truncateForLog(s string) string {
	if len(s) > 16 {
		return s[:8] + "…" + s[len(s)-8:]
	}
	return s
}
