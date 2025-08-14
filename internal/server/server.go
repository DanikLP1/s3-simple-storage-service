package server

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/DanikLP1/s3-storage-service/internal/db"
	"github.com/DanikLP1/s3-storage-service/internal/storage"
)

type Server struct {
	db      *db.DB
	storage *storage.Storage
	Logger  *slog.Logger
}

func New(database *db.DB, d storage.StorageDriver, logger *slog.Logger) *Server {
	return &Server{
		db:      database,
		storage: storage.NewWithDriver(d),
		Logger:  logger,
	}
}

// Router возвращает http.Handler, который вешается в main.go
// internal/server/router.go
func (s *Server) Router() http.Handler {
	mux := http.NewServeMux()

	// Health/Ready (полезно для k8s, можно убрать если не нужно)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.db.DB.Exec("SELECT 1").Error; err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	// Главный маршрутизатор S3 API
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Корень: список бакетов
		if r.URL.Path == "/" {
			if r.Method == http.MethodGet {
				s.handleListBuckets(w, r)
				return
			}
			writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "only GET on /", r.URL.Path, "")
			return
		}

		// helpers
		hasLifecycle := func() bool {
			// Go 1.21+: r.URL.Query().Has("lifecycle")
			q := r.URL.Query()
			if _, ok := q["lifecycle"]; ok {
				return true
			}
			// также ловим варианты типа ?lifecycle=1
			return q.Get("lifecycle") != ""
		}

		p := strings.Trim(r.URL.Path, "/")
		parts := strings.SplitN(p, "/", 2)

		// -------- Bucket-level --------
		if len(parts) == 1 {
			bucket := parts[0]

			// S3 lifecycle: /:bucket?lifecycle
			if hasLifecycle() {
				switch r.Method {
				case http.MethodPut:
					s.handlePutBucketLifecycle(w, r, bucket) // читает XML из тела, сохраняет правила
					return
				case http.MethodGet:
					s.handleGetBucketLifecycle(w, r, bucket) // отдаёт XML или 404 NoSuchLifecycleConfiguration
					return
				case http.MethodDelete:
					s.handleDeleteBucketLifecycle(w, r, bucket) // удаляет правила, 204
					return
				default:
					writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported lifecycle method", r.URL.Path, "")
					return
				}
			}

			// Обычные bucket-операции
			switch r.Method {
			case http.MethodPut:
				s.handlePutBucket(w, r, bucket) // create bucket
				return
			case http.MethodDelete:
				s.handleDeleteBucket(w, r, bucket)
				return
			case http.MethodGet:
				// ListObjectsV2
				if r.URL.Query().Get("list-type") == "2" {
					s.handleListObjectsV2(w, r, bucket)
					return
				}
				// Можно вернуть NotImplemented, если V1 не поддерживаешь
				writeS3Error(w, http.StatusNotImplemented, "NotImplemented", "list objects not implemented", r.URL.Path, "")
				return
			default:
				writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported method for bucket", r.URL.Path, "")
				return
			}
		}

		// -------- Object-level (bucket/key) --------
		switch r.Method {
		case http.MethodPut:
			s.handlePut(w, r)
			return
		case http.MethodGet:
			s.handleGet(w, r)
			return
		case http.MethodDelete:
			s.handleDelete(w, r)
			return
		case http.MethodHead:
			// HEAD отдаёт только заголовки (у тебя handleGet уже это умеет — без тела при 304/412 и т.п.)
			s.handleGet(w, r)
			return
		default:
			writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported method", r.URL.Path, "")
			return
		}
	})

	return mux
}
