package storage

import (
	"context"
	"io"
)

type Storage struct {
	driver StorageDriver
}

func NewWithDriver(d StorageDriver) *Storage {
	return &Storage{driver: d}
}

func (s *Storage) Driver() StorageDriver {
	return s.driver
}

func (s *Storage) Put(ctx context.Context, id string, r io.Reader, size int64, checksum []byte) error {
	ws, err := s.driver.BeginWrite(ctx, BlobID(id), PutOpts{Size: size, Checksum: checksum})
	if err != nil {
		return err
	}
	if _, err = io.Copy(ws.Writer(), r); err != nil {
		_ = ws.Abort(ctx)
		return err
	}
	return ws.Commit(ctx)
}

func (s *Storage) ReadAt(ctx context.Context, id string, off int64, n int64) (io.ReadCloser, error) {
	return s.driver.ReadAt(ctx, BlobID(id), off, n)
}

func (s *Storage) Stat(ctx context.Context, id string) (int64, bool, error) {
	return s.driver.Stat(ctx, BlobID(id))
}

func (s *Storage) Delete(ctx context.Context, id string) error {
	return s.driver.Delete(ctx, BlobID(id))
}
