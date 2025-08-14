package db

import (
	"fmt"

	"gorm.io/gorm"
)

type DB struct {
	*gorm.DB
}

func New(gormDB *gorm.DB) *DB { return &DB{gormDB} }

func (db *DB) AutoMigrate() error {
	if err := db.DB.AutoMigrate(&Bucket{}, &Blob{}, &Object{}, &ObjectVersion{}, &User{}, &IdempotencyKey{}, &LifecycleRule{}); err != nil {
		return err
	}
	return db.ensureIndexes()
}

func (db *DB) ensureIndexes() error {
	stmts := []string{
		// --- blobs ---
		`CREATE UNIQUE INDEX IF NOT EXISTS ux_blobs_checksum ON blobs (checksum)`,
		`CREATE INDEX IF NOT EXISTS ix_blobs_state ON blobs (state)`,
		`CREATE INDEX IF NOT EXISTS ix_blobs_state_created ON blobs (state, created_at)`,
		`CREATE INDEX IF NOT EXISTS ix_blobs_storage_node ON blobs (storage_node)`,

		// --- objects ---
		// GORM уже держит уникальность по (bucket_id,key) через теги, но индекс явный не помешает.
		`CREATE INDEX IF NOT EXISTS ix_objects_bucket_key ON objects (bucket_id, key)`,
		`CREATE INDEX IF NOT EXISTS ix_objects_bucket_key_head ON objects (bucket_id, key, head_version_id)`,

		// --- object_versions ---
		// для быстрых листингов и поиска предыдущих версий
		`CREATE INDEX IF NOT EXISTS ix_objvers_bucket_key_created_desc ON object_versions (bucket_id, key, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS ix_objvers_bucket_key_isdel_created ON object_versions (bucket_id, key, is_delete, created_at)`,
		`CREATE INDEX IF NOT EXISTS ix_objvers_blob_id ON object_versions (blob_id)`,

		// --- lifecycle_rules ---
		`CREATE INDEX IF NOT EXISTS ix_lifecycle_bucket_prefix_enabled ON lifecycle_rules (bucket_id, prefix, enabled)`,
	}

	for i, s := range stmts {
		if err := db.DB.Exec(s).Error; err != nil {
			return fmt.Errorf("ensureIndexes step %d failed: %w", i, err)
		}
	}
	return nil
}

func (db *DB) DSN(path string) string {
	// WAL + FK + нормальная синхронизация
	return fmt.Sprintf("%s?_journal_mode=WAL&_foreign_keys=on&_busy_timeout=5000", path)
}
