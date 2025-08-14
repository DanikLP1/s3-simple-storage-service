// internal/server/gc.go (новый файл, если удобно)
package server

import (
	"context"
	"time"

	"log/slog"
)

func (s *Server) StartGC(ctx context.Context, every time.Duration, batch int) {
	log := s.Logger.With(slog.String("comp", "gc"))

	go func() {
		log.Info("gc.started", "every", every.String(), "batch", batch)
		t := time.NewTicker(every)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Info("gc.stopped", "reason", "context canceled")
				return
			case <-t.C:
				start := time.Now()
				totalFiles := 0
				var totalBytes int64 = 0

				rows, err := s.db.BlobsForGCWithSize(batch)
				if err != nil {
					log.Error("gc.query_fail", "err", err)
					continue
				}
				if len(rows) == 0 {
					log.Info("gc.nothing_to_do")
					continue
				}

				log.Info("gc.pass_begin", "candidates", len(rows))
				for _, r := range rows {
					// удаляем байты
					if err := s.storage.Delete(ctx, r.ID); err != nil {
						log.Error("gc.storage_delete_fail", "blob_id", r.ID, "err", err)
						// пропускаем удаление записи — попробуем в следующий проход
						continue
					}
					// удаляем запись
					if err := s.db.DeleteBlobRecordTx(s.db.DB, r.ID); err != nil {
						log.Error("gc.db_delete_fail", "blob_id", r.ID, "err", err)
						// это не критично: байты уже удалены, но запись добьём на следующем проходе
						continue
					}

					totalFiles++
					totalBytes += r.Size
					log.Info("gc.deleted", "blob_id", r.ID, "size", r.Size)
				}

				log.Info("gc.pass_end",
					"deleted_files", totalFiles,
					"freed_bytes", totalBytes,
					"dur_ms", time.Since(start).Milliseconds(),
				)
			}
		}
	}()
}
