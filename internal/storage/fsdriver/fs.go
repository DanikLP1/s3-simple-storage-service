package fsdriver

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/DanikLP1/s3-storage-service/internal/storage"
	"github.com/oklog/ulid/v2"
)

type FS struct {
	Root string
}

func New(root string) *FS { return &FS{Root: root} }

func (fs *FS) pathFor(id storage.BlobID) (dir, tmp, final string) {
	s := strings.ReplaceAll(string(id), "-", "")
	if len(s) < 4 {
		s = fmt.Sprintf("%-4s", s)
	}
	a, b := s[:2], s[2:4]
	dir = filepath.Join(fs.Root, "blobs", a, b)
	final = filepath.Join(dir, string(id)+".bin")
	tmp = final + ".tmp-" + ulid.Make().String()
	return
}

type writeSession struct {
	tmpPath   string
	finalPath string
	dirPath   string
	f         *os.File
	w         io.Writer
	expectSum []byte
	expectSz  int64
	written   int64
}

func (fs *FS) BeginWrite(ctx context.Context, id storage.BlobID, opts storage.PutOpts) (storage.WriteSession, error) {
	dir, tmp, final := fs.pathFor(id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	ws := &writeSession{
		tmpPath:   tmp,
		finalPath: final,
		dirPath:   dir,
		f:         f,
		w:         f,
		expectSum: opts.Checksum,
		expectSz:  opts.Size,
	}
	return ws, nil
}

func (ws *writeSession) Writer() io.Writer { return ws }

func (ws *writeSession) Write(p []byte) (int, error) {
	n, err := ws.f.Write(p)
	ws.written += int64(n)
	return n, err
}

func (ws *writeSession) Commit(ctx context.Context) error {
	if err := ws.f.Sync(); err != nil {
		_ = ws.f.Close()
		_ = os.Remove(ws.tmpPath)
		return err
	}
	if err := ws.f.Close(); err != nil {
		_ = os.Remove(ws.tmpPath)
		return err
	}
	if err := os.Rename(ws.tmpPath, ws.finalPath); err != nil {
		_ = os.Remove(ws.tmpPath)
		return err
	}

	dir, err := os.Open(ws.dirPath)
	if err != nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func (ws *writeSession) Abort(ctx context.Context) error {
	_ = ws.f.Close()
	return (os.Remove(ws.tmpPath))
}

func (fs *FS) ReadAt(ctx context.Context, id storage.BlobID, off int64, n int64) (io.ReadCloser, error) {
	_, _, final := fs.pathFor(id)
	f, err := os.Open(final)
	if err != nil {
		return nil, err
	}
	if off > 0 {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	if n >= 0 {
		return struct {
			io.Reader
			io.Closer
		}{Reader: io.LimitReader(f, n), Closer: f}, nil
	}
	return f, nil
}

func (fs *FS) Stat(ctx context.Context, id storage.BlobID) (int64, bool, error) {
	_, _, final := fs.pathFor(id)
	fi, err := os.Stat(final)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return fi.Size(), true, nil
}

func (fs *FS) Delete(ctx context.Context, id storage.BlobID) error {
	_, _, final := fs.pathFor(id)
	err := os.Remove(final)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
