// db/idempotency_repo.go
package db

import (
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (db *DB) SaveIdempotencyTx(tx *gorm.DB, bucketID uint, key, idemKey, versionID, etag string) error {
	item := IdempotencyKey{
		BucketID: bucketID, Key: key, IdemKey: idemKey,
		VersionID: versionID, ETag: etag,
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&item).Error
}

func (db *DB) GetIdempotencyTx(tx *gorm.DB, bucketID uint, key, idemKey string) (string, string, error) {
	var item IdempotencyKey
	err := tx.Where("bucket_id = ? AND key = ? AND idem_key = ?", bucketID, key, idemKey).First(&item).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", "", ErrNotFound
	}
	return item.VersionID, item.ETag, err
}
