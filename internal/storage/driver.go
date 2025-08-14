package storage

import (
	"context"
	"io"
)

type BlobID string

type PutOpts struct {
	Size     int64
	Checksum []byte
}

type StorageDriver interface {
	BeginWrite(ctx context.Context, id BlobID, opts PutOpts) (WriteSession, error)
	ReadAt(ctx context.Context, id BlobID, off int64, n int64) (io.ReadCloser, error)
	Stat(ctx context.Context, id BlobID) (size int64, exists bool, err error)
	Delete(ctx context.Context, id BlobID) error
}

type WriteSession interface {
	Writer() io.Writer
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error
}
