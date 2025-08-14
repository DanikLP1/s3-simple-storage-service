package logging

import (
	"log/slog"
	"os"
	"strings"
)

type Config struct {
	Level string // "debug"|"info"|"warn"|"error"
	JSON  bool   // true -> JSON, false -> text
}

var Log *slog.Logger

func New(cfg Config) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(cfg.Level) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	var h slog.Handler
	if cfg.JSON {
		h = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	} else {
		h = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	}
	return slog.New(h)
}
