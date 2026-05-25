package main

import (
	"os"
	"strconv"
	"time"
)

type config struct {
	port             int
	latencyThreshold float64
	queueMaxSize     int
	queueTimeout     time.Duration
	ewmaAlpha        float64
	stateLogInterval time.Duration
}

func loadConfig() *config {
	return &config{
		port:             envInt("KVROUTER_PORT", 3000),
		latencyThreshold: envFloat("KVROUTER_LATENCY_THRESHOLD", 3.0),
		queueMaxSize:     envInt("KVROUTER_QUEUE_MAX_SIZE", 1000),
		queueTimeout:     time.Duration(envFloat("KVROUTER_QUEUE_TIMEOUT", 1200)) * time.Second,
		ewmaAlpha:        envFloat("KVROUTER_EWMA_ALPHA", 0.3),
		stateLogInterval: time.Duration(envFloat("KVROUTER_STATE_LOG_INTERVAL", 30)) * time.Second,
	}
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
