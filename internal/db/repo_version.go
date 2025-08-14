package db

import (
	"errors"
	"time"

	"gorm.io/gorm"
)

type VersionMeta struct {
	VersionID   string
	BucketID    uint
	Key         string
	BlobID      *string
	Size        *int64
	ETag        *string
	ContentType *string
	IsDelete    bool
	CreatedAt   time.Time
}

func (db *DB) InsertObjectVersionTx(tx *gorm.DB, bucketID uint, key, versionID, blobID string, size int64, etag, contentType string) error {
	ver := ObjectVersion{
		VersionID: versionID, BucketID: bucketID, Key: key,
		BlobID: &blobID, Size: &size, ETag: &etag, ContentType: &contentType,
		IsDelete: false,
	}
	return tx.Create(&ver).Error
}

func (db *DB) CreateDeleteMarkerTx(tx *gorm.DB, bucketID uint, key, versionID string) error {
	ver := ObjectVersion{VersionID: versionID, BucketID: bucketID, Key: key, IsDelete: true}
	return tx.Create(&ver).Error
}

func (db *DB) SetHeadVersionTx(tx *gorm.DB, bucketID uint, key, versionID string) error {
	return tx.Model(&Object{}).
		Where("bucket_id = ? AND key = ?", bucketID, key).
		Update("head_version_id", versionID).Error
}

func (db *DB) GetHeadVersionTx(tx *gorm.DB, bucketID uint, key string) (*ObjectVersion, error) {
	var obj Object
	if err := tx.Where("bucket_id = ? AND key = ?", bucketID, key).First(&obj).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var ver ObjectVersion
	if err := tx.Where("version_id = ?", obj.HeadVersionID).First(&ver).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &ver, nil
}

func (db *DB) GetVersionTx(tx *gorm.DB, versionID string) (*ObjectVersion, error) {
	var ver ObjectVersion
	err := tx.Where("version_id = ?", versionID).First(&ver).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	return &ver, err
}

func (db *DB) GetPrevVersionTx(tx *gorm.DB, bucketID uint, key, currentVersionID string) (*ObjectVersion, error) {
	var ver ObjectVersion
	err := tx.Where("bucket_id = ? AND key = ? AND version_id <> ?", bucketID, key, currentVersionID).
		Order("created_at DESC").First(&ver).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	return &ver, err
}

func (db *DB) DeleteVersionTx(tx *gorm.DB, versionID string) error {
	return tx.Delete(&ObjectVersion{VersionID: versionID}).Error
}

func (db *DB) CreateVersionTx(tx *gorm.DB, bucketID uint, key, versionID, blobID string,
	size int64, etag, contentType string) error {
	return tx.Create(&ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucketID,
		Key:         key,
		BlobID:      &blobID,
		Size:        &size,
		ETag:        &etag,
		ContentType: &contentType,
		IsDelete:    false,
	}).Error
}

func (db *DB) CreateVersion(bucketID uint, key, versionID, blobID string,
	size int64, etag, contentType string) error {
	return db.Create(&ObjectVersion{
		VersionID: versionID, BucketID: bucketID, Key: key,
		BlobID: &blobID, Size: &size, ETag: &etag, ContentType: &contentType,
		IsDelete: false, CreatedAt: time.Now().UTC(),
	}).Error
}

func (db *DB) CreateDeleteMarker(bucketID uint, key, versionID string) error {
	return db.Create(&ObjectVersion{
		VersionID: versionID, BucketID: bucketID, Key: key,
		BlobID: nil, Size: nil, ETag: nil, ContentType: nil,
		IsDelete: true, CreatedAt: time.Now().UTC(),
	}).Error
}

func (db *DB) GetHeadVersion(bucketID uint, key string) (*VersionMeta, error) {
	var o Object
	if err := db.Where("bucket_id=? AND `key`=?", bucketID, key).Take(&o).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if o.HeadVersionID == "" {
		return nil, ErrNotFound
	}
	return db.GetVersion(o.HeadVersionID)
}

func (db *DB) GetVersion(versionID string) (*VersionMeta, error) {
	var v ObjectVersion
	if err := db.Where("version_id=?", versionID).Take(&v).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &VersionMeta{
		VersionID: v.VersionID, BucketID: v.BucketID, Key: v.Key,
		BlobID: v.BlobID, Size: v.Size, ETag: v.ETag,
		ContentType: v.ContentType, IsDelete: v.IsDelete, CreatedAt: v.CreatedAt,
	}, nil
}

func (db *DB) DeleteVersion(versionID string) error {
	return db.Delete(&ObjectVersion{}, "version_id = ?", versionID).Error
}

func (db *DB) BlobRefCountFromVersions(blobID string) (int64, error) {
	var n int64
	err := db.Model(&ObjectVersion{}).Where("blob_id = ?", blobID).Count(&n).Error
	return n, err
}

func (db *DB) GetPrevVersion(bucketID uint, key, excludeVersionID string) (*VersionMeta, error) {
	var v ObjectVersion
	err := db.
		Where("bucket_id=? AND `key`=? AND version_id <> ?", bucketID, key, excludeVersionID).
		Order("created_at DESC").
		Take(&v).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &VersionMeta{
		VersionID: v.VersionID, BucketID: v.BucketID, Key: v.Key,
		BlobID: v.BlobID, Size: v.Size, ETag: v.ETag, ContentType: v.ContentType,
		IsDelete: v.IsDelete, CreatedAt: v.CreatedAt,
	}, nil
}
