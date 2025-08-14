package db

import (
	"crypto/rand"
	"encoding/hex"
	"errors"

	"gorm.io/gorm"
)

var ErrNotFound = errors.New("not found")
var ErrBucketNotEmpty = errors.New("bucket not empty")
var ErrInvalidContToken = errors.New("can't validate continuation token")
var ErrAccessDenied = errors.New("access denied")

func genHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func derefInt64(p *int64) int64 {
	if p != nil {
		return *p
	}
	return 0
}

func (db *DB) GenBlobID() string    { return genHex(20) } // 40 hex
func (db *DB) GenVersionID() string { return genHex(16) } // позже для версий

func (db *DB) WithTx(fn func(tx *gorm.DB) error) error {
	return db.DB.Transaction(func(tx *gorm.DB) error { return fn(tx) })
}

// ВАЖНО: не используй gorm.Transaction внутри этой функции для sqlite!
func (db *DB) WithTxImmediate(fn func(tx *gorm.DB) error) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		return fn(tx)
	})
}
