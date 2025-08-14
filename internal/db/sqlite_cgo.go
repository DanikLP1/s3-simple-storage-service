//go:build !cgo

package db

import (
	"github.com/glebarez/sqlite" // 👈 вместо gorm.io/driver/sqlite
	"gorm.io/gorm"
)

func OpenSQLite(path string) (*DB, error) {
	g, err := gorm.Open(sqlite.Open((&DB{}).DSN(path)), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	db := New(g)
	return db, db.AutoMigrate()
}
