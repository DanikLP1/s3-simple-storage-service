package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/DanikLP1/s3-storage-service/internal/db"
	"github.com/DanikLP1/s3-storage-service/internal/storage"
	"gorm.io/gorm"
)

func parseBucketKey(path string) (bucket, key string, err error) {
	p := strings.Trim(path, "/") // 👈 убираем и в начале, и в конце
	parts := strings.SplitN(p, "/", 2)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid path, expected /:bucket/:key")
	}
	return parts[0], parts[1], nil
}

func stripQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	bucket, key, err := parseBucketKey(r.URL.Path)
	log := loggerFrom(r).With(slog.String("bucket", bucket), slog.String("key", key))
	log.Info("put_object.start")
	if err != nil {
		log.Warn("put_object.bad_path", "err", err)
		writeS3Error(w, http.StatusBadRequest, "InvalidRequest", err.Error(), r.URL.Path, requestIDFrom(r))
		return
	}

	ownerID := getUserIDFromCtx(r.Context())
	bucketID, err := s.db.EnsureBucket(bucket, ownerID)
	if err != nil {
		log.Error("put_object.ensure_bucket_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "bucket error", r.URL.Path, requestIDFrom(r))
		return
	}

	// ---- 1) IO вне транзакции: стримим байты в storage и считаем хэш ----
	newBlobID := s.db.GenBlobID()
	ws, err := s.storage.Driver().BeginWrite(r.Context(), storage.BlobID(newBlobID), storage.PutOpts{Size: r.ContentLength})
	if err != nil {
		log.Error("put_object.beginwrite_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "write begin error", r.URL.Path, requestIDFrom(r))
		return
	}

	hasher := sha256.New()
	written, copyErr := io.Copy(ws.Writer(), io.TeeReader(r.Body, hasher))
	if copyErr != nil {
		_ = ws.Abort(r.Context())
		log.Error("put_object.write_fail", "err", copyErr)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "write error", r.URL.Path, requestIDFrom(r))
		return
	}
	if err := ws.Commit(r.Context()); err != nil {
		log.Error("put_object.commit_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "commit error", r.URL.Path, requestIDFrom(r))
		return
	}

	size := written
	sumHex := hex.EncodeToString(hasher.Sum(nil))
	checksum := "sha256:" + sumHex
	etag := `"` + checksum + `"`
	ctype := r.Header.Get("Content-Type")
	if ctype == "" {
		ctype = "application/octet-stream"
	}

	// базовые валидации сразу
	if r.ContentLength >= 0 && size != r.ContentLength {
		log.Warn("put_object.bad_length", "got", size, "want", r.ContentLength)
		_ = s.storage.Delete(r.Context(), newBlobID) // зачистим запись на диске
		writeS3Error(w, http.StatusBadRequest, "BadDigest", "mismatched content length", r.URL.Path, requestIDFrom(r))
		return
	}
	if want := r.Header.Get("x-amz-content-sha256"); want != "" && want != sumHex && want != "UNSIGNED-PAYLOAD" {
		log.Warn("put_object.bad_sha256", "want", want, "got", sumHex)
		_ = s.storage.Delete(r.Context(), newBlobID)
		writeS3Error(w, http.StatusBadRequest, "BadDigest", "sha256 mismatch", r.URL.Path, requestIDFrom(r))
		return
	}

	idem := r.Header.Get("X-Idempotency-Key")
	if idem != "" {
		log.Info("put_object.idem_key", "idem_key", idem)
	}

	// результат txn, чтобы отдать после коммита
	type putResult struct {
		versionID string
		etag      string
		blobID    string
		size      int64
		status    int
	}
	var res putResult

	staged := true
	usedNew := false

	// ---- 2) Транзакция: лок ключа, дедуп, метаданные, идемпотентность ----
	if err := s.db.WithTxImmediate(func(tx *gorm.DB) error {
		if err := s.db.LockObjectForUpdate(tx, bucketID, key); err != nil {
			log.Error("put_object.lock_fail", "err", err)
			return err
		}

		// идемпотентность (после лока!)
		if idem != "" {
			if verID, e, err := s.db.GetIdempotencyTx(tx, bucketID, key, idem); err == nil {
				// ранний возврат (но из txn нельзя писать в http) — просто сформируем res
				log.Info("put_object.idem_hit", "version_id", verID, "etag", e)
				res = putResult{versionID: verID, etag: e, status: http.StatusOK}
				// этот путь НЕ меняет метаданные; txn закроется успешно
				return nil
			} else if !errors.Is(err, db.ErrNotFound) {
				log.Error("put_object.idem_lookup_fail", "err", err)
				return err
			}
		}

		// дедуп по checksum
		var useBlobID string
		var useSize int64
		if exist, err := s.db.FindBlobByChecksumTx(tx, checksum); err == nil && exist != nil {
			// нашли готовый blob — удаляем только что записанную копию
			_ = s.storage.Delete(r.Context(), newBlobID)
			staged = false
			useBlobID, useSize = exist.ID, exist.Size
			log.Info("put_object.dedup_hit", "blob_id", useBlobID, "size", useSize)
		} else if err != nil && !errors.Is(err, db.ErrNotFound) {
			_ = s.storage.Delete(r.Context(), newBlobID)
			log.Error("put_object.find_checksum_fail", "err", err)
			return err
		} else {
			// резервируем и помечаем ready новый blob
			if err := s.db.ReserveBlobPendingTx(tx, newBlobID, checksum, size, "local"); err != nil {
				_ = s.storage.Delete(r.Context(), newBlobID)
				log.Error("put_object.reserve_blob_fail", "err", err)
				return err
			}
			if err := s.db.MarkBlobReadyTx(tx, newBlobID); err != nil {
				log.Error("put_object.mark_ready_fail", "err", err)
				return err
			}
			usedNew = true
			useBlobID, useSize = newBlobID, size
			log.Info("put_object.blob_ready", "blob_id", useBlobID, "size", useSize)
		}

		verID := s.db.GenVersionID()
		if err := s.db.InsertObjectVersionTx(tx, bucketID, key, verID, useBlobID, useSize, etag, ctype); err != nil {
			log.Error("put_object.create_version_fail", "err", err)
			return err
		}
		if err := s.db.UpsertObjectTx(tx, bucketID, key, useBlobID, useSize, etag, ctype, verID); err != nil {
			log.Error("put_object.upsert_obj_fail", "err", err)
			return err
		}
		if err := s.db.SetHeadVersionTx(tx, bucketID, key, verID); err != nil {
			log.Error("put_object.set_head_fail", "err", err)
			return err
		}

		// сохраняем идемпотентный ответ
		if idem != "" {
			if err := s.db.SaveIdempotencyTx(tx, bucketID, key, idem, verID, etag); err != nil {
				log.Warn("put_object.idem_save_warn", "err", err)
			}
		}

		res = putResult{
			versionID: verID,
			etag:      etag,
			blobID:    useBlobID,
			size:      useSize,
			status:    http.StatusOK,
		}
		return nil
	}); err != nil {
		if staged {
			_ = s.storage.Delete(r.Context(), newBlobID)
			staged = false
		}
		if !errors.Is(err, context.Canceled) {
			log.Error("put_object.tx_fail", "err", err)
		}
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "tx error", r.URL.Path, requestIDFrom(r))
		return
	}

	if staged && !usedNew {
		_ = s.storage.Delete(r.Context(), newBlobID)
		staged = false
	}

	// ---- 3) HTTP‑ответ уже после успешной txn ----
	if res.versionID != "" {
		w.Header().Set("ETag", res.etag)
		w.Header().Set("x-amz-version-id", res.versionID)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(res.status)
		log.Info("put_object.ok", "blob_id", res.blobID, "size", res.size, "version_id", res.versionID)
		return
	}

	// идемпотентный HIT: заголовки уже есть в res
	w.Header().Set("ETag", res.etag)
	w.Header().Set("x-amz-version-id", res.versionID)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	log.Info("put_object.idem_ok", "version_id", res.versionID)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	bucket, key, err := parseBucketKey(r.URL.Path)
	log := loggerFrom(r).With(slog.String("bucket", bucket), slog.String("key", key))
	log.Info("get_object.start")
	if err != nil {
		log.Warn("get_object.bad_path", "err", err)
		writeS3Error(w, http.StatusBadRequest, "InvalidRequest", err.Error(), r.URL.Path, requestIDFrom(r))
		return
	}

	ownerID := getUserIDFromCtx(r.Context())
	bucketID, err := s.db.BucketIDByName(bucket, ownerID)
	if errors.Is(err, db.ErrNotFound) {
		log.Warn("get_object.no_such_bucket")
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", "/"+bucket, requestIDFrom(r))
		return
	}
	if err != nil {
		log.Error("get_object.bucket_lookup_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", r.URL.Path, requestIDFrom(r))
		return
	}

	versionID := r.URL.Query().Get("versionId")
	var ver *db.ObjectVersion
	if versionID == "" {
		ver, err = s.db.GetHeadVersionTx(s.db.DB, bucketID, key)
	} else {
		ver, err = s.db.GetVersionTx(s.db.DB, versionID)
	}
	if errors.Is(err, db.ErrNotFound) || (ver != nil && ver.IsDelete) {
		log.Info("get_object.not_found", "version_id", versionID)
		writeS3Error(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", r.URL.Path, requestIDFrom(r))
		return
	}
	if err != nil {
		log.Error("get_object.db_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", r.URL.Path, requestIDFrom(r))
		return
	}

	b, err := s.db.GetBlob(*ver.BlobID)
	if err != nil {
		log.Error("get_object.blob_missing", "blob_id", *ver.BlobID, "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "blob missing", r.URL.Path, requestIDFrom(r))
		return
	}

	// предикаты
	if ver.ETag != nil {
		ifMatch := r.Header.Get("If-Match")
		if ifMatch != "" && stripQuotes(ifMatch) != stripQuotes(*ver.ETag) {
			log.Info("get_object.precondition_failed", "if_match", ifMatch, "etag", *ver.ETag)
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		ifNone := r.Header.Get("If-None-Match")
		if ifNone != "" && stripQuotes(ifNone) == stripQuotes(*ver.ETag) {
			log.Info("get_object.not_modified", "etag", *ver.ETag)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", *ver.ETag)
	}
	w.Header().Set("x-amz-version-id", ver.VersionID)

	ct := "application/octet-stream"
	if ver.ContentType != nil && *ver.ContentType != "" {
		ct = *ver.ContentType
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Accept-Ranges", "bytes")

	// Range
	total := b.Size
	var start, length int64 = 0, -1
	status := http.StatusOK
	if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
		spec := strings.TrimPrefix(rng, "bytes=")
		var a, z string
		if i := strings.IndexByte(spec, '-'); i >= 0 {
			a, z = spec[:i], spec[i+1:]
		}
		switch {
		case a != "" && z != "":
			as, _ := strconv.ParseInt(a, 10, 64)
			bs, _ := strconv.ParseInt(z, 10, 64)
			if as < 0 || bs < as || as >= total {
				log.Warn("get_object.bad_range", "range", rng, "size", total)
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			start, length, status = as, bs-as+1, http.StatusPartialContent
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", as, bs, total))
		case a != "" && z == "":
			as, _ := strconv.ParseInt(a, 10, 64)
			if as < 0 || as >= total {
				log.Warn("get_object.bad_range", "range", rng, "size", total)
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			start, length, status = as, total-as, http.StatusPartialContent
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", as, total-1, total))
		case a == "" && z != "":
			zs, _ := strconv.ParseInt(z, 10, 64)
			if zs <= 0 {
				log.Warn("get_object.bad_range", "range", rng)
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if zs > total {
				zs = total
			}
			start, length, status = total-zs, zs, http.StatusPartialContent
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, total-1, total))
		}
		log.Info("get_object.range", "start", start, "length", length, "total", total)
	}

	rc, err := s.storage.ReadAt(r.Context(), *ver.BlobID, start, length)
	if err != nil {
		log.Error("get_object.read_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "read error", r.URL.Path, requestIDFrom(r))
		return
	}
	defer rc.Close()

	if length >= 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", length))
	} else {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", total))
	}
	w.WriteHeader(status)
	n, _ := io.Copy(w, rc)
	log.Info("get_object.ok", "blob_id", *ver.BlobID, "version_id", ver.VersionID, "status", status, "bytes", n)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	bucket, key, err := parseBucketKey(r.URL.Path)
	log := loggerFrom(r).With(slog.String("bucket", bucket), slog.String("key", key))
	log.Info("delete_object.start")
	if err != nil {
		log.Warn("delete_object.bad_path", "err", err)
		writeS3Error(w, http.StatusBadRequest, "InvalidRequest", err.Error(), r.URL.Path, requestIDFrom(r))
		return
	}

	ownerID := getUserIDFromCtx(r.Context())
	bucketID, err := s.db.BucketIDByName(bucket, ownerID)
	if errors.Is(err, db.ErrNotFound) {
		log.Warn("delete_object.no_such_bucket")
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", "/"+bucket, requestIDFrom(r))
		return
	}
	if err != nil {
		log.Error("delete_object.bucket_lookup_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", r.URL.Path, requestIDFrom(r))
		return
	}

	versionID := r.URL.Query().Get("versionId")

	type delResult struct {
		returnVersion string
		status        int
	}
	var res delResult

	if err := s.db.WithTxImmediate(func(tx *gorm.DB) error {
		if err := s.db.LockObjectForUpdate(tx, bucketID, key); err != nil {
			log.Error("delete_object.lock_fail", "err", err)
			return err
		}

		// 1) Без versionId — мягкое удаление (delete‑marker)
		if versionID == "" {
			dm := s.db.GenVersionID()
			if err := s.db.CreateDeleteMarkerTx(tx, bucketID, key, dm); err != nil {
				log.Error("delete_object.create_dm_fail", "err", err)
				return err
			}
			if err := s.db.SetHeadVersionTx(tx, bucketID, key, dm); err != nil {
				log.Error("delete_object.set_head_fail", "err", err)
				return err
			}
			res = delResult{returnVersion: dm, status: http.StatusNoContent}
			log.Info("delete_object.ok_delete_marker", "version_id", dm)
			return nil
		}

		// 2) С versionId — удаление указанной версии
		ver, err := s.db.GetVersionTx(tx, versionID)
		if errors.Is(err, db.ErrNotFound) {
			log.Warn("delete_object.no_such_version", "version_id", versionID)
			// В txn нельзя писать ответ — просто вернём «мягкую» ошибку наружу
			res = delResult{status: http.StatusNotFound}
			return nil
		}
		if err != nil {
			log.Error("delete_object.get_version_fail", "err", err)
			return err
		}

		if err := s.db.DeleteVersionTx(tx, versionID); err != nil {
			log.Error("delete_object.delete_version_fail", "err", err)
			return err
		}

		// Если это был HEAD — переставить HEAD на предыдущую (или на delete‑marker)
		head, _ := s.db.GetHeadVersionTx(tx, bucketID, key)
		if head == nil || head.VersionID == versionID {
			if prev, err := s.db.GetPrevVersionTx(tx, bucketID, key, versionID); err == nil && prev != nil {
				_ = s.db.SetHeadVersionTx(tx, bucketID, key, prev.VersionID)
				log.Info("delete_object.head_moved", "new_head", prev.VersionID)
			} else {
				dm := s.db.GenVersionID()
				_ = s.db.CreateDeleteMarkerTx(tx, bucketID, key, dm)
				_ = s.db.SetHeadVersionTx(tx, bucketID, key, dm)
				log.Info("delete_object.head_set_dm", "dm", dm)
			}
		}

		// GC блоба, если осиротел
		if ver.BlobID != nil {
			if cnt, _ := s.db.BlobRefCountFromVersionsTx(tx, *ver.BlobID); cnt == 0 {
				_ = s.storage.Delete(r.Context(), *ver.BlobID)
				_ = s.db.DeleteBlobRecordTx(tx, *ver.BlobID)
				log.Info("delete_object.blob_gc", "blob_id", *ver.BlobID)
			}
		}

		res = delResult{returnVersion: versionID, status: http.StatusNoContent}
		log.Info("delete_object.ok", "version_id", versionID)
		return nil
	}); err != nil {
		log.Error("delete_object.tx_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "tx error", r.URL.Path, requestIDFrom(r))
		return
	}

	// ответы после txn
	if res.status == http.StatusNotFound {
		writeS3Error(w, http.StatusNotFound, "NoSuchVersion", "The specified version does not exist.", r.URL.Path, requestIDFrom(r))
		return
	}
	w.Header().Set("x-amz-version-id", res.returnVersion)
	w.WriteHeader(res.status)
}
