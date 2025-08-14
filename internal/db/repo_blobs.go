package db

import (
	"errors"
	"time"

	"gorm.io/gorm"
)

type BlobMeta struct {
	ID          string
	Path        string
	Size        int64
	Checksum    string
	StorageNode string
	CreatedAt   time.Time
}

type GCBlob struct {
	ID   string
	Size int64
}

func (db *DB) FindBlobByChecksumTx(tx *gorm.DB, checksum string) (*Blob, error) {
	var b Blob
	if err := tx.Where("checksum = ? AND state = ?", checksum, "ready").First(&b).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &b, nil
}

func (db *DB) CreateBlobTx(tx *gorm.DB, blobID string, path string, size int64, checksum, state string) error {
	// path можешь передавать "" (мы от него ушли логически)
	return tx.Create(&Blob{
		ID: blobID, Path: path, Size: size, Checksum: checksum,
		StorageNode: "local", // пока одиночный инстанс
	}).Error
}

func (db *DB) ReserveBlobPendingTx(tx *gorm.DB, id, checksum string, size int64, storageNode string) error {
	return tx.Create(&Blob{
		ID: id, Checksum: checksum, Size: size, State: "pending", StorageNode: storageNode,
	}).Error
}

func (db *DB) MarkBlobReadyTx(tx *gorm.DB, id string) error {
	return tx.Model(&Blob{}).Where("id = ?", id).Update("state", "ready").Error
}

func (db *DB) DeleteBlobRecordTx(tx *gorm.DB, id string) error {
	return tx.Delete(&Blob{ID: id}).Error
}

func (db *DB) BlobRefCountFromVersionsTx(tx *gorm.DB, blobID string) (int64, error) {
	var cnt int64
	if err := tx.Model(&ObjectVersion{}).Where("blob_id = ?", blobID).Count(&cnt).Error; err != nil {
		return 0, err
	}
	return cnt, nil
}

func (db *DB) CreateBlob(id, path string, size int64, checksum, storageNode string) error {
	return db.Create(&Blob{
		ID: id, Path: path, Size: size, Checksum: checksum,
		StorageNode: storageNode, CreatedAt: time.Now().UTC(),
	}).Error
}

func (db *DB) GetBlob(id string) (*BlobMeta, error) {
	var b Blob
	if err := db.Take(&b, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &BlobMeta{
		ID: b.ID, Path: b.Path, Size: b.Size, Checksum: b.Checksum,
		StorageNode: b.StorageNode, CreatedAt: b.CreatedAt,
	}, nil
}

func (db *DB) FindBlobByChecksum(checksum string) (*BlobMeta, error) {
	var b Blob
	if err := db.Where("checksum = ?", checksum).Limit(1).Take(&b).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &BlobMeta{
		ID: b.ID, Path: b.Path, Size: b.Size, Checksum: b.Checksum,
		StorageNode: b.StorageNode, CreatedAt: b.CreatedAt,
	}, nil
}

func (db *DB) ListObjectVersionBlobIDs(bucketID uint, key string) ([]string, error) {
	var vers []ObjectVersion
	if err := db.Where("bucket_id = ? AND `key` = ?", bucketID, key).
		Select("blob_id").Find(&vers).Error; err != nil {
		return nil, err
	}
	uniq := make(map[string]struct{})
	for _, v := range vers {
		if v.BlobID != nil && *v.BlobID != "" {
			uniq[*v.BlobID] = struct{}{}
		}
	}
	out := make([]string, 0, len(uniq))
	for id := range uniq {
		out = append(out, id)
	}
	return out, nil
}

func (db *DB) BlobRefCount(blobID string) (int64, error) {
	var n int64
	err := db.Model(&Object{}).Where("blob_id = ?", blobID).Count(&n).Error
	return n, err
}

// GC / pending
// BlobsForGCWithSize возвращает до limit блобов, на которые нет ссылок версий (is_delete=false)
// и которые уже в состоянии 'ready'.
func (db *DB) BlobsForGCWithSize(limit int) ([]GCBlob, error) {
	var rows []GCBlob
	err := db.DB.Raw(`
		SELECT b.id, b.size
		FROM blobs b
		LEFT JOIN object_versions v ON v.blob_id = b.id AND v.is_delete = FALSE
		WHERE v.blob_id IS NULL AND b.state='ready'
		LIMIT ?
	`, limit).Scan(&rows).Error
	return rows, err
}
