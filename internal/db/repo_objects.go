package db

import (
	"context"
	"encoding/base64"
	"errors"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ListV2Params struct {
	BucketID     uint
	Prefix       string
	Delimiter    string
	MaxKeys      int
	StartAfter   string
	ContTokenRaw string // continuation-token (base64)
	FetchOwner   bool
	EncodingType string // "url" or ""
}

type ListV2Item struct {
	Key          string
	ETag         *string
	Size         int64
	LastModified time.Time
	OwnerID      *string
	OwnerName    *string
}

type ListV2Result struct {
	Objects        []ListV2Item
	CommonPrefixes []string
	IsTruncated    bool
	NextToken      string
	KeyCount       int
}

// простейший токен — последнее ключевое имя
type contToken struct {
	LastKey string `json:"k"`
}

type ObjectMeta struct {
	BlobID      string
	Size        int64
	ETag        string
	ContentType string
}

func (db *DB) LockObjectForUpdate(tx *gorm.DB, bucketID uint, key string) error {
	res := tx.Exec(`UPDATE objects SET key = key WHERE bucket_id = ? AND key = ?`, bucketID, key)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected > 0 {
		return nil
	}

	// Создаём строку, если её не было
	obj := Object{BucketID: bucketID, Key: key}
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "bucket_id"}, {Name: "key"}},
		DoNothing: true,
	}).Create(&obj).Error; err != nil {
		return err
	}
	return tx.Exec(`UPDATE objects SET key = key WHERE bucket_id = ? AND key = ?`, bucketID, key).Error
}

func (db *DB) UpsertObjectTx(tx *gorm.DB, bucketID uint, key, blobID string, size int64, etag, contentType string, headVersionID string) error {
	obj := Object{
		BucketID: bucketID, Key: key,
		BlobID: blobID, Size: size, ETag: etag, ContentType: contentType,
		HeadVersionID: headVersionID,
	}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "bucket_id"}, {Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"blob_id", "size", "e_tag", "content_type", "head_version_id"}),
	}).Create(&obj).Error
}

func (db *DB) FindObject(bucketID uint, key string) (*ObjectMeta, error) {
	var o Object
	if err := db.Where("bucket_id = ? AND key = ?", bucketID, key).Take(&o).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &ObjectMeta{
		BlobID: o.BlobID, Size: o.Size, ETag: o.ETag, ContentType: o.ContentType,
	}, nil
}

func (db *DB) ListObjectsV2(ctx context.Context, p ListV2Params) (*ListV2Result, error) {
	if p.MaxKeys <= 0 || p.MaxKeys > 1000 {
		p.MaxKeys = 1000
	}
	afterKey := p.StartAfter
	if p.ContTokenRaw != "" {
		// токен главнее start-after
		p.StartAfter = ""
		b, err := base64.RawURLEncoding.DecodeString(p.ContTokenRaw)
		if err != nil {
			return nil, ErrInvalidContToken
		}
		afterKey = string(b)
	}

	type row struct {
		Key          string    `gorm:"column:key"`
		ETag         *string   `gorm:"column:e_tag"`
		Size         *int64    `gorm:"column:size"`
		LastModified time.Time `gorm:"column:last_modified"`
	}

	q := db.
		Model(&Object{}).
		// ВАЖНО: join по ov.version_id (а не ov.id)
		Select(`
			objects.key AS key,
			ov.e_tag    AS e_tag,
			ov.size     AS size,
			ov.created_at AS last_modified
		`).
		Joins(`JOIN object_versions ov ON ov.version_id = objects.head_version_id`).
		Where("objects.bucket_id = ?", p.BucketID).
		Where("ov.is_delete = ?", false)

	if p.Prefix != "" {
		q = q.Where("objects.key LIKE ?", p.Prefix+"%")
	}
	if afterKey != "" {
		q = q.Where("objects.key > ?", afterKey)
	}

	q = q.Order("objects.key ASC").Limit(p.MaxKeys + 1)

	var rows []row
	if err := q.WithContext(ctx).Scan(&rows).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return &ListV2Result{}, nil
		}
		return nil, err
	}

	// формирование результата
	result := &ListV2Result{}
	if p.Delimiter != "" {
		prefixSet := make(map[string]struct{})
		for _, r := range rows {
			rest := strings.TrimPrefix(r.Key, p.Prefix)
			if idx := strings.Index(rest, p.Delimiter); idx >= 0 {
				cp := p.Prefix + rest[:idx+1]
				prefixSet[cp] = struct{}{}
				continue
			}
			result.Objects = append(result.Objects, ListV2Item{
				Key:          r.Key,
				ETag:         r.ETag,
				Size:         derefInt64(r.Size),
				LastModified: r.LastModified.UTC(),
			})
		}
		for cp := range prefixSet {
			result.CommonPrefixes = append(result.CommonPrefixes, cp)
		}
		sort.Strings(result.CommonPrefixes)
	} else {
		for _, r := range rows {
			result.Objects = append(result.Objects, ListV2Item{
				Key:          r.Key,
				ETag:         r.ETag,
				Size:         derefInt64(r.Size),
				LastModified: r.LastModified.UTC(),
			})
		}
	}

	if len(rows) > p.MaxKeys || len(result.Objects)+len(result.CommonPrefixes) > p.MaxKeys {
		result.IsTruncated = true

		// Обрезаем видимые элементы до лимита (учитывая префиксы)
		trimTo := p.MaxKeys - len(result.CommonPrefixes)
		if trimTo < 0 {
			trimTo = 0
		}
		if len(result.Objects) > trimTo {
			result.Objects = result.Objects[:trimTo]
		}

		// Токен — base64(rawurl) от последнего возвращённого объекта,
		// если объектов нет — можно использовать последний префикс (приблизительно).
		var lastKey string
		if n := len(result.Objects); n > 0 {
			lastKey = result.Objects[n-1].Key
		} else if n := len(result.CommonPrefixes); n > 0 {
			// это компромисс: AWS использует внутренний ключ,
			// у нас допустимо взять последний CommonPrefix
			lastKey = result.CommonPrefixes[n-1]
		}
		if lastKey != "" {
			result.NextToken = base64.RawURLEncoding.EncodeToString([]byte(lastKey))
		}
	}

	// Итоговый счётчик
	result.KeyCount = len(result.Objects) + len(result.CommonPrefixes)
	return result, nil
}

func (db *DB) ClearObjectHeadMeta(bucketID uint, key string) error {
	return db.DB.Model(&Object{}).
		Where("bucket_id = ? AND `key` = ?", bucketID, key).
		Updates(map[string]any{
			"blob_id":      "",
			"size":         0,
			"e_tag":        `""`,
			"content_type": "",
		}).Error
}
