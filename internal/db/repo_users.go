package db

import (
	"errors"

	"gorm.io/gorm"
)

func (db *DB) EnsureUser(accessKeyID, secret string) (uint, error) {
	var u User
	if err := db.Where("access_key_id = ?", accessKeyID).Take(&u).Error; err == nil {
		return u.ID, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, err
	}
	u = User{AccessKeyID: accessKeyID, SecretAccessKey: secret, Status: "active"}
	if err := db.Create(&u).Error; err != nil {
		return 0, err
	}
	return u.ID, nil
}

func (db *DB) FindUserByAccessKey(id string) (*User, error) {
	var u User
	if err := db.Where("access_key_id = ? AND status = 'active'", id).Take(&u).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &u, nil
}

func (db *DB) FindUserByID(id uint) (*User, error) {
	var u User
	if err := db.Where("id = ?", id).Take(&u).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &u, nil
}
