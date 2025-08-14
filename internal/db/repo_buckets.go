package db

import (
	"errors"

	"gorm.io/gorm"
)

// EnsureBucket — найти или создать
func (db *DB) EnsureBucket(name string, ownerID uint) (uint, error) {
	b := Bucket{Name: name}
	// идемпотентное создание: если есть — вернёт существующий, если нет — создаст
	if err := db.DB.Where("name = ?", name).FirstOrCreate(&b).Error; err != nil {
		return 0, err
	}

	if b.OwnerID == 0 && ownerID != 0 {
		_ = db.DB.Model(&b).Update("owner_id", ownerID).Error
	}
	return b.ID, nil
}

func (db *DB) BucketIDByName(name string, ownerID uint) (uint, error) {
	var b Bucket
	if err := db.Where("name = ? AND owner_id = ?", name, ownerID).Take(&b).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, ErrNotFound
		}
		return 0, err
	}
	return b.ID, nil
}

func (db *DB) ListBuckets(ownerID uint) ([]Bucket, error) {
	var out []Bucket
	q := db.DB.Model(&Bucket{})
	if ownerID != 0 {
		q = q.Where("owner_id = ?", ownerID)
	}
	if err := q.Order("name asc").Find(&out).Error; err != nil {
		return nil, err
	}
	return out, nil
}

func (db *DB) DeleteBucketIfEmpty(tx *gorm.DB, bucketID uint) error {
	var n int64

	// Есть ли объекты (HEAD-строки)?
	if err := tx.Model(&Object{}).Where("bucket_id = ?", bucketID).Count(&n).Error; err != nil {
		return err
	}
	if n > 0 {
		return ErrBucketNotEmpty
	}
	// Есть ли версии?
	if err := tx.Model(&ObjectVersion{}).Where("bucket_id = ?", bucketID).Count(&n).Error; err != nil {
		return err
	}
	if n > 0 {
		return ErrBucketNotEmpty
	}
	// Удаляем бакет
	if err := tx.Delete(&Bucket{}, bucketID).Error; err != nil {
		return err
	}
	return nil
}
