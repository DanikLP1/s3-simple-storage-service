package config

import (
	"log"
	"os"
	"strconv"
)

type Config struct {
	Addr          string // ":8080"
	DataDir       string // "./data"
	DBDSN         string // "file:meta.db?_busy_timeout=5000&_fk=1"
	Region        string // "us-east-1"
	LogLevel      string // "info"
	MaxClockSkewS int    // 900 (15 мин)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func New() Config {
	cfg := Config{
		Addr:          getenv("PORT", ":8080"),
		DataDir:       getenv("DATA_DIR", "./data"),
		DBDSN:         getenv("DB_DSN", "file:meta.db?_busy_timeout=5000&_fk=1"),
		Region:        getenv("REGION", "us-east-1"),
		LogLevel:      getenv("LOG_LEVEL", "info"),
		MaxClockSkewS: 900,
	}
	if v := os.Getenv("MAX_CLOCK_SKEW_S"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxClockSkewS = n
		} else {
			log.Printf("invalid MAX_CLOCK_SKEW_S: %v", err)
		}
	}
	return cfg
}
