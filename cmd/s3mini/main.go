package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/DanikLP1/s3-storage-service/internal/db"
	"github.com/DanikLP1/s3-storage-service/internal/logging"
	"github.com/DanikLP1/s3-storage-service/internal/server"
	"github.com/DanikLP1/s3-storage-service/internal/storage/fsdriver"
)

func main() {
	database, err := db.OpenSQLite("meta.db")
	if err != nil {
		log.Fatal("DB error:", err)
	}

	if err := database.AutoMigrate(); err != nil {
		log.Fatalf("Migration error: %v", err)
	}

	logger := logging.New(logging.Config{
		Level: "info",
		JSON:  true,
	})

	drv := fsdriver.New("data")

	srv := server.New(database, drv, logger)
	addr := ":8080"
	mux := srv.Router()
	handler := srv.WithRecover(srv.WithRequestLogger(srv.AuthMiddleware(mux)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv.StartGC(ctx, 15*time.Minute, 256)

	go srv.StartLifecycle(ctx, 15*time.Minute, 50)

	fmt.Println("Listening on http://localhost" + addr)
	if err := http.ListenAndServe(addr, server.WrapWriteCheck(handler)); err != nil {
		log.Fatal(err)
	}
}
