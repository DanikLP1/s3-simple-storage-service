package db

import "time"

// Bucket — один на имя
type Bucket struct {
	ID        uint      `gorm:"primaryKey"`
	Name      string    `gorm:"uniqueIndex;size:255;not null"`
	OwnerID   uint      `gorm:"index;"`
	CreatedAt time.Time `gorm:"autoCreateTime"`

	User User `gorm:"foreignKey:OwnerID;references:ID;constraint:OnDelete:SET NULL"`
}

// Blob — физические байты
type Blob struct {
	ID          string    `gorm:"primaryKey;size:64"` // hex/uuid
	StorageNode string    `gorm:"size:64;index"`
	Path        string    `gorm:"not null"`
	Size        int64     `gorm:"not null"`
	Checksum    string    `gorm:"index;size:80"`               // "sha256:...."
	State       string    `gorm:"size:16;index;default:ready"` // pending|ready
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

// Object — логический объект, указывает на Blob
type Object struct {
	ID            uint      `gorm:"primaryKey"`
	BucketID      uint      `gorm:"uniqueIndex:ux_bucket_key,priority:1;not null"`
	Key           string    `gorm:"uniqueIndex:ux_bucket_key,priority:2;size:2048;not null"`
	BlobID        string    `gorm:"index;size:64;not null"`
	Size          int64     `gorm:"not null"`
	ETag          string    `gorm:"size:96;not null"` // "\"sha256:...\""
	ContentType   string    `gorm:"size:255"`
	HeadVersionID string    `gorm:"index;size:64"`
	CreatedAt     time.Time `gorm:"autoCreateTime"`

	// Опционально: связи
	Bucket Bucket `gorm:"foreignKey:BucketID;constraint:OnDelete:CASCADE"`
	Blob   Blob   `gorm:"foreignKey:BlobID;references:ID;constraint:OnDelete:RESTRICT"`
}

// ObjectVersion - версия объекта, указывает на блоб
type ObjectVersion struct {
	VersionID   string  `gorm:"primaryKey;size:64"` // hex/uuid
	BucketID    uint    `gorm:"index:idx_ver_bucket_key,priority:1;not null"`
	Key         string  `gorm:"index:idx_ver_bucket_key,priority:2;size:2048;not null"`
	BlobID      *string `gorm:"index;size:64"` // NULL => delete-marker
	Size        *int64
	ETag        *string   `gorm:"size:96"`
	ContentType *string   `gorm:"size:255"`
	IsDelete    bool      `gorm:"not null;default:false"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

// User - пользователь для SigV4
type User struct {
	ID              uint      `gorm:"primaryKey"`
	AccessKeyID     string    `gorm:"uniqueIndex;size:64;not null"`
	SecretAccessKey string    `gorm:"size:128;not null"`
	Status          string    `gorm:"size:16;default:active"`
	CreatedAt       time.Time `gorm:"autoCreateTime"`
}

// IdempotencyKey — ключ идемпотентности для PUT
type IdempotencyKey struct {
	BucketID  uint      `gorm:"primaryKey"`
	Key       string    `gorm:"primaryKey;size:2048"`
	IdemKey   string    `gorm:"primaryKey;size:128"`
	VersionID string    `gorm:"size:64;not null"`
	ETag      string    `gorm:"size:96;not null"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

type LifecycleRule struct {
	ID       uint   `gorm:"primary:key"`
	BucketID uint   `gorm:"index;not null"`
	Prefix   string `gorm:"size:1024;default:''"`
	Enabled  bool   `gorm:"default:true"`
	//Actions
	ExpireCurrentAfterDays        *int `gorm:""` // N дней не обновлялся -> delete-marker
	ExpireNoncurrentAfterDays     *int `gorm:""` // удалить версии старше X дней
	NoncurrentNewerVersionsToKeep *int `gorm:""` // оставить K свежих версий (опц.)
	PurgeDeleteMarkersAfterDays   *int `gorm:""` // чистить delete-markers старше Y дней
	// на будущее
	// TransitionToClass string  // "cold", "archive", ...
	// TransitionAfterDays *int
	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`

	Bucket Bucket `gorm:"foreignKey:BucketID;constraint:OnDelete:CASCADE"`
}
