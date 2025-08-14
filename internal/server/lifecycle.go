package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/DanikLP1/s3-storage-service/internal/db"
	"github.com/DanikLP1/s3-storage-service/internal/logging"
	"gorm.io/gorm"
)

type LifecycleWorker struct {
	s      *Server
	Every  time.Duration
	Batch  int
	logger *slog.Logger
}

func (s *Server) StartLifecycle(ctx context.Context, every time.Duration, batch int) {
	lw := &LifecycleWorker{
		s: s, Every: every, Batch: batch,
		logger: logging.New(logging.Config{Level: "info", JSON: true}).With(slog.String("comp", "lifecycle")),
	}
	go lw.run(ctx)
}

func (lw *LifecycleWorker) run(ctx context.Context) {
	t := time.NewTicker(lw.Every)
	defer t.Stop()
	lw.logger.Info("lifecycle.started", "every", lw.Every.String(), "batch", lw.Batch)

	for {
		select {
		case <-ctx.Done():
			lw.logger.Info("lyfecycle.stopped")
			return
		case <-t.C:
			lw.onePass(ctx)
		}
	}
}

func (lw *LifecycleWorker) onePass(ctx context.Context) {
	start := time.Now()
	rules, err := lw.s.db.ListEnabledLifecycleRules()
	if err != nil {
		lw.logger.Error("rules_load_fail", "err", err)
		return
	}
	if len(rules) == 0 {
		lw.logger.Info("no_rules")
		return
	}

	var totalChanged int
	for _, rule := range rules {
		rlog := lw.logger.With(
			slog.Uint64("bucket_id", uint64(rule.BucketID)),
			slog.String("prefix", rule.Prefix),
		)
		rlog.Info("rule_begin")

		// 1) Noncurrent expiration: по возрасту
		if rule.ExpireNoncurrentAfterDays != nil && *rule.ExpireNoncurrentAfterDays >= 0 {
			cut := time.Now().AddDate(0, 0, -*rule.ExpireNoncurrentAfterDays)
			vers, err := lw.s.db.ListNoncurrentByAge(rule.BucketID, rule.Prefix, cut, lw.Batch)
			if err != nil {
				rlog.Error("noncurrent_query_fail", "err", err)
			} else {
				changed := lw.deleteVersionsTx(ctx, vers, "noncurrent_deleted")
				totalChanged += changed
				if changed > 0 {
					rlog.Info("noncurrent_deleted", "count", changed)
				}
			}
		}

		// 1b) Nucurrent keep newest K
		if rule.NoncurrentNewerVersionsToKeep != nil && *rule.NoncurrentNewerVersionsToKeep >= 0 {
			vers, err := lw.s.db.ListNoncurrentKeepNewest(rule.BucketID, rule.Prefix, *rule.NoncurrentNewerVersionsToKeep, lw.Batch)
			if err != nil {
				rlog.Error("noncurrent_keep_query_fail", "err", err)
			} else {
				changed := lw.deleteVersionsTx(ctx, vers, "nocurrent_pruned")
				totalChanged += changed
				if changed > 0 {
					rlog.Info("noncurrent_pruned", "count", changed, "keep", *rule.NoncurrentNewerVersionsToKeep)
				}
			}
		}

		// 2) Purge delete-markers
		if rule.PurgeDeleteMarkersAfterDays != nil && *rule.PurgeDeleteMarkersAfterDays >= 0 {
			cut := time.Now().AddDate(0, 0, -*rule.PurgeDeleteMarkersAfterDays)
			dms, err := lw.s.db.ListDeleteMarkersForPurge(rule.BucketID, rule.Prefix, cut, lw.Batch)
			if err != nil {
				rlog.Error("dm_query_fail", "err", err)
			} else {
				changed := lw.purgeDeleteMarkersTx(dms)
				totalChanged += changed
				if changed > 0 {
					rlog.Info("dm_purged", "count", changed)
				}
			}
		}

		// 3) Expire current (HEAD) - ставим delete-marker
		if rule.ExpireCurrentAfterDays != nil && *rule.ExpireCurrentAfterDays >= 0 {
			cut := time.Now().AddDate(0, 0, -*rule.PurgeDeleteMarkersAfterDays)
			objs, err := lw.s.db.ListHeadsOlderThan(rule.BucketID, rule.Prefix, cut, lw.Batch)
			if err != nil {
				rlog.Error("head_query_fail", "err", err)
			} else {
				changed := lw.expireCurrentTx(objs)
				totalChanged += changed
				if changed > 0 {
					rlog.Info("current_expired", "count", changed)
				}
			}
		}

		rlog.Info("rule_end")
	}
	lw.logger.Info("pass_end", "changed", totalChanged, "dur_ms", time.Since(start).Milliseconds())
}

// --------------------- шаги в транзакциях --------------------------

func (lw *LifecycleWorker) deleteVersionsTx(ctx context.Context, vers []db.ObjectVersion, event string) int {
	changed := 0
	for _, v := range vers {
		_ = lw.s.db.WithTxImmediate(func(tx *gorm.DB) error {
			// лочим объект
			if err := lw.s.db.LockObjectForUpdate(tx, v.BucketID, v.Key); err != nil {
				lw.logger.Error("lock_fail", "key", v.Key, "err", err)
				return err
			}
			// удаляем версию
			if err := lw.s.db.DeleteVersionTx(tx, v.VersionID); err != nil {
				lw.logger.Error("delete_version_fail", "version_id", v.VersionID, "err", err)
				return err
			}
			// GC блоба, если осирател
			if v.BlobID != nil {
				if cnt, _ := lw.s.db.BlobRefCountFromVersionsTx(tx, *v.BlobID); cnt == 0 {
					_ = lw.s.storage.Delete(ctx, *v.BlobID)
					_ = lw.s.db.DeleteBlobRecordTx(tx, *v.BlobID)
					lw.logger.Info("g.deleted", "blob_id", *v.BlobID)
				}
			}
			changed++
			lw.logger.Info(event, "key", v.Key, "version_id", v.VersionID)
			return nil
		})
	}
	return changed
}

func (lw *LifecycleWorker) purgeDeleteMarkersTx(dms []db.ObjectVersion) int {
	changed := 0
	for _, dm := range dms {
		_ = lw.s.db.WithTxImmediate(func(tx *gorm.DB) error {
			// лочим объект
			if err := lw.s.db.LockObjectForUpdate(tx, dm.BucketID, dm.Key); err != nil {
				lw.logger.Error("lock_fail", "key", dm.Key, "err", err)
				return err
			}
			head, _ := lw.s.db.GetHeadVersionTx(tx, dm.BucketID, dm.Key)
			// Не трогаем объект, если это текущий HEAD (иначе слуайно оживим объект)
			if head != nil && head.VersionID == dm.VersionID {
				return nil
			}
			if err := lw.s.db.DeleteVersionTx(tx, dm.VersionID); err != nil {
				lw.logger.Error("dm_delete_fail", "version_id", dm.VersionID, "err", err)
				return err
			}
			changed++
			lw.logger.Info("dm_purged", "key", dm.Key, "version_id", dm.VersionID)
			return nil
		})
	}
	return changed
}

func (lw *LifecycleWorker) expireCurrentTx(objs []db.Object) int {
	changed := 0
	for _, o := range objs {
		_ = lw.s.db.WithTxImmediate(func(tx *gorm.DB) error {
			// лочим объект
			if err := lw.s.db.LockObjectForUpdate(tx, o.BucketID, o.Key); err != nil {
				lw.logger.Error("lock_fail", "key", o.Key, "err", err)
				return err
			}
			dm := lw.s.db.GenVersionID()
			if err := lw.s.db.CreateDeleteMarkerTx(tx, o.BucketID, o.Key, dm); err != nil {
				lw.logger.Error("dm_create_fail", "key", o.Key, "err", err)
				return err
			}
			if err := lw.s.db.SetHeadVersionTx(tx, o.BucketID, o.Key, dm); err != nil {
				lw.logger.Error("set_head_fail", "key", o.Key, "err", err)
				return err
			}
			changed++
			lw.logger.Info("current_expired", "key", o.Key, "dm", dm)
			return nil
		})
	}
	return changed
}
