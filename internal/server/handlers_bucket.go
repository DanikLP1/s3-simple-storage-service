package server

import (
	"encoding/xml"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/DanikLP1/s3-storage-service/internal/db"
	"gorm.io/gorm"
)

// GET /  -> {"buckets":[{"name":"...","createdAt":"..."}]}
func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	log := loggerFrom(r)
	log.Info("list_buckets.start")

	ownerID := getUserIDFromCtx(r.Context())

	u, err := s.db.FindUserByID(ownerID)
	if err != nil {
		log.Error("list_buckets.get_user.fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", r.URL.Path, requestIDFrom(r))
	}

	// Получаем все бакеты (добавь соответствующий метод в repo)
	bs, err := s.db.ListBuckets(ownerID)
	if err != nil {
		log.Error("list_buckets.db_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", r.URL.Path, requestIDFrom(r))
		return
	}
	out := make([]S3Bucket, 0, len(bs))
	for _, b := range bs {
		out = append(out, S3Bucket{
			Name: b.Name, CreationDate: b.CreatedAt.UTC(),
		})
	}
	// Owner — заглушка
	writeListBuckets(w, strconv.FormatUint(uint64(u.ID), 10), "local", out)
	log.Info("list_buckets.ok", "count", len(out))
}

// PUT /:bucket  -> создать, если нет
func (s *Server) handlePutBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	log := loggerFrom(r).With(slog.String("bucket", bucket))

	log.Info("create_bucket.start")

	if bucket == "" {
		log.Warn("invalid bucket name")
		writeS3Error(w, http.StatusBadRequest, "InvalidBucketName", "empty bucket name", r.URL.Path, requestIDFrom(r))
		return
	}

	ownerID := getUserIDFromCtx(r.Context())

	id, err := s.db.EnsureBucket(bucket, ownerID)
	if err != nil {
		// Важный момент: сюда уже не прилетит ErrRecordNotFound — FirstOrCreate сам создаст
		log.Error("create_bucket.db_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", err.Error(), r.URL.Path, requestIDFrom(r))
		return
	}
	// идемпотентный успех
	w.Header().Set("Location", "/"+bucket)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	log.Info("create_bucket.ok", "bucket_id", id)
}

// DELETE /:bucket  -> удалить, если пуст
func (s *Server) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	log := loggerFrom(r).With(slog.String("bucket", bucket))
	log.Info("delete_bucket.start")

	if bucket == "" {
		log.Warn("delete_bucket.invalid_name")
		http.Error(w, "empty bucket name", http.StatusBadRequest)
		return
	}

	ownerID := getUserIDFromCtx(r.Context())

	bucketID, err := s.db.BucketIDByName(bucket, ownerID)
	switch {
	case errors.Is(err, db.ErrNotFound):
		log.Warn("delete_bucket.no_such_bucket")
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", "/"+bucket, requestIDFrom(r))
		return
	case err != nil:
		log.Error("delete_bucket.db_fail_lookup", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", r.URL.Path, requestIDFrom(r))
		return
	}

	err = s.db.WithTxImmediate(func(tx *gorm.DB) error {
		if err := s.db.DeleteBucketIfEmpty(tx, bucketID); err != nil {
			return err
		}
		return nil
	})

	if errors.Is(err, db.ErrBucketNotEmpty) {
		log.Warn("delete_bucket.not_empty")
		writeS3Error(
			w, http.StatusConflict,
			"BucketNotEmpty", "The bucket you tried to delete is not empty.",
			"/"+bucket, "",
		)
		return
	}
	log.Error("delete_bucket.db_fail_delete", "err", err)
	writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", r.URL.Path, requestIDFrom(r))
	return

	w.WriteHeader(http.StatusNoContent) // 204, без тела
	log.Info("delete_bucket.ok", "bucket_id", bucketID)
}

// ----------------- Bucket Lifecycles -------------------------

func (s *Server) handlePutBucketLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	log := loggerFrom(r).With(slog.String("bucket", bucket))
	log.Info("lyfecycle.put.start")

	ownerID := getUserIDFromCtx(r.Context())
	bucketID, err := s.db.BucketIDByName(bucket, ownerID)
	switch {
	case errors.Is(err, db.ErrNotFound):
		log.Warn("lyfecycle.put.no_such_bucket")
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", "/"+bucket, requestIDFrom(r))
		return
	case err != nil:
		log.Error("lyfecycle.put.db_fail_lookup", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", r.URL.Path, requestIDFrom(r))
		return
	}

	var cfg LifecycleConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&cfg); err != nil {
		log.Warn("lifecycle.put.bad_xml", "err", err)
		writeS3Error(w, http.StatusBadRequest, "MalformedXML", "cannot parse lifecycle xml", r.URL.Path, requestIDFrom(r))
		return
	}

	if err := s.db.WithTx(func(tx *gorm.DB) error {
		if err := tx.Where("bucket_id = ?", bucketID).Delete(&db.LifecycleRule{}).Error; err != nil {
			return err
		}
		for _, xr := range cfg.Rules {
			rule := ruleFromXML(bucketID, xr)
			if err := tx.Create(&rule).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		log.Error("lifecycle.put.save_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", r.URL.Path, requestIDFrom(r))
		return
	}

	w.WriteHeader(http.StatusOK)
	log.Info("lifecycle.put.ok", "rules", len(cfg.Rules))
}

func (s *Server) handleGetBucketLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	log := loggerFrom(r).With(slog.String("bucket", bucket))
	log.Info("lifecycle.get.start")

	ownerID := getUserIDFromCtx(r.Context())
	bucketID, err := s.db.BucketIDByName(bucket, ownerID)
	switch {
	case errors.Is(err, db.ErrNotFound):
		log.Warn("lifecycle.get.no_such_bucket")
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", "/"+bucket, requestIDFrom(r))
		return
	case err != nil:
		log.Error("lifecycle.get.db_fail_lookup", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", r.URL.Path, requestIDFrom(r))
		return
	}

	var rules []db.LifecycleRule
	if err := s.db.DB.Where("bucket_id = ?", bucketID).Find(&rules).Error; err != nil {
		log.Error("lifecycle.get.db_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", r.URL.Path, requestIDFrom(r))
		return
	}

	if len(rules) == 0 {
		log.Info("lifecycle.get.empty")
		writeS3Error(w, http.StatusNotFound, "NoSuchLifecycleConfiguration",
			"The lifecycle configuration does not exist.", r.URL.Path, requestIDFrom(r))
		return
	}

	cfg := LifecycleConfiguration{Rules: make([]Rule, 0, len(rules))}
	for _, r := range rules {
		cfg.Rules = append(cfg.Rules, ruleToXML(r))
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	if err := xml.NewEncoder(w).Encode(cfg); err != nil {
		log.Error("lifecycle.get.encode_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "Can't write response to XML", r.URL.Path, requestIDFrom(r))
	}
	log.Info("lifecycle.get.ok", "rules", len(rules))
}

func (s *Server) handleDeleteBucketLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	log := loggerFrom(r).With(slog.String("bucket", bucket))
	log.Info("lifecycle.delete.start")

	ownerID := getUserIDFromCtx(r.Context())
	bucketID, err := s.db.BucketIDByName(bucket, ownerID)
	switch {
	case errors.Is(err, db.ErrNotFound):
		log.Warn("lifecycle.delete.no_such_bucket")
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", "/"+bucket, requestIDFrom(r))
		return
	case err != nil:
		log.Error("lifecycle.delete.db_fail_lookup", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", r.URL.Path, requestIDFrom(r))
		return
	}

	if err := s.db.DB.Where("bucket_id = ?", bucketID).Delete(&db.LifecycleRule{}).Error; err != nil {
		log.Error("lifecycle.delete.db_fail", "err", err)
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "db error", r.URL.Path, requestIDFrom(r))
		return
	}
	w.WriteHeader(http.StatusNoContent)
	log.Info("lifecycle.delete.ok")
}
