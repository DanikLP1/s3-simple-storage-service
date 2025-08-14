package db

import "time"

func (db *DB) ListEnabledLifecycleRules() ([]LifecycleRule, error) {
	var rules []LifecycleRule
	err := db.DB.Where("enabled = ?", true).Find(&rules).Error
	return rules, err
}

func (db *DB) ListNoncurrentByAge(bucketID uint, prefix string, olderThan time.Time, limit int) ([]ObjectVersion, error) {
	var vers []ObjectVersion
	err := db.DB.
		Where("bucket_id = ? AND key LIKE ? AND is_delete = FALSE AND created_at < ?", bucketID, prefix+"%", olderThan).
		Order("created_at ASC").
		Limit(limit).
		Find(&vers).Error
	return vers, err
}

// Для SQLite: вернуть самые старые noncurrent-версии СВЕРХ K свежих.
// Алгоритм:
//  1) Найти ключи, где число noncurrent-версий > keep.
//  2) Для каждого ключа взять версии, отсортированные по created_at DESC,
//     с OFFSET keep (то есть «всё после K свежих»), пока не наберём limit.
func (db *DB) ListNoncurrentKeepNewest(bucketID uint, prefix string, keep int, limit int) ([]ObjectVersion, error) {
	type KeyCnt struct {
		Key string
		Cnt int64
	}
	keys := []KeyCnt{}

	// 1) ключи с избытком noncurrent-версий
	// Важно: исключаем HEAD для каждого key

	q := `
		SELECT v.key AS key, COUNT(*) AS cnt
		FROM object_versions v
		JOIN objects o
		  ON o.bucket_id = v.bucket_id AND o.key = v.key
		WHERE v.bucket_id = ? AND v.key LIKE ? AND v.is_delete = FALSE
		  AND v.version_id <> o.head_version_id
		GROUP BY v.key
		HAVING COUNT(*) > ?
		ORDER BY v.key
	`
	if err := db.DB.Raw(q, bucketID, prefix+"%", keep).Scan(&keys).Error; err != nil {
		return nil, err
	}
	if len(keys) == 0 || limit <= 0 {
		return []ObjectVersion{}, nil
	}

	vers := make([]ObjectVersion, 0, min(limit, 256))
	left := limit

	// 2) по каждому ключу добавим старые версии с OFFSET keep
	for _, kc := range keys {
		if left <= 0 {
			break
		}
		// берем до left версий (можно дробить по ключам)
		var rows []ObjectVersion
		// сортируем от новых к старым, но берем все ПОСЛЕ keep
		// добавляем вторичный порядок по version_id для стабильности
		err := db.DB.
			Raw(`
				SELECT v.version_id, v.bucket_id, v.key, v.blob_id
				FROM object_versions v
				JOIN objects o
				  ON o.bucket_id = v.bucket_id AND o.key = v.key
				WHERE v.bucket_id = ? AND v.key = ? AND v.is_delete = FALSE
				  AND v.version_id <> o.head_version_id
				ORDER BY v.created_at DESC, v.version_id DESC
				LIMIT ? OFFSET ?
			`, bucketID, kc.Key, left, keep).
			Scan(&rows).Error
		if err != nil {
			return nil, err
		}
		vers = append(vers, rows...)
		left -= len(rows)
	}

	return vers, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (db *DB) ListDeleteMarkersForPurge(bucketID uint, prefix string, olderThan time.Time, limit int) ([]ObjectVersion, error) {
	var dms []ObjectVersion
	err := db.DB.
		Where("bucket_id = ? AND key LIKE ? AND is_delete = TRUE AND created_at < ?", bucketID, prefix+"%", olderThan).
		Order("created_at ASC").
		Limit(limit).
		Find(&dms).Error
	return dms, err
}

func (db *DB) ListHeadsOlderThan(bucketID uint, prefix string, olderThan time.Time, limit int) ([]Object, error) {
	var objs []Object
	err := db.DB.
		Where("bucket_id = ? AND key LIKE ? AND created_at < ?", bucketID, prefix+"%", olderThan).
		Order("created_at ASC").
		Limit(limit).
		Find(&objs).Error
	return objs, err
}
