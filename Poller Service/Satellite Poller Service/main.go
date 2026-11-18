package main

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultSatelliteBaseURL       = "https://satellite.naufarrel.dev"
	DefaultReconnectDelay         = 5 * time.Second
	DefaultMaxLateness            = 30 * time.Minute
	DefaultSQLiteDBPath           = "/var/lib/satellite-poller/state.db"
	DefaultKafkaBrokers           = "kafka-aggregate:29092"
	DefaultKafkaTopicHazardIngest = "hazard-ingest"
	DefaultHealthPort             = "8085"
)

type Config struct {
	SatelliteBaseURL       string
	SatelliteGroupToken    string
	ReconnectDelay         time.Duration
	MaxLateness            time.Duration
	SQLiteDBPath           string
	KafkaBrokers           []string
	KafkaTopicHazardIngest string
	HealthPort             string
}

// Load reads and validates application configuration
// from environment variables.
func Load() (*Config, error) {
	token, err := requiredEnv("SATELLITE_GROUP_TOKEN")
	if err != nil {
		return nil, err
	}

	reconnectDelay, err := durationOrDefault(
		"SATELLITE_RECONNECT_DELAY_SECONDS",
		DefaultReconnectDelay,
	)
	if err != nil {
		return nil, err
	}

	maxLateness, err := durationOrDefault(
		"MAX_LATENESS_MINUTES",
		DefaultMaxLateness,
	)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		SatelliteBaseURL:       envOrDefault("SATELLITE_BASE_URL", DefaultSatelliteBaseURL),
		SatelliteGroupToken:    token,
		ReconnectDelay:         reconnectDelay,
		MaxLateness:            maxLateness,
		SQLiteDBPath:           envOrDefault("SQLITE_DB_PATH", DefaultSQLiteDBPath),
		KafkaBrokers:           csvEnvOrDefault("KAFKA_BROKERS", DefaultKafkaBrokers),
		KafkaTopicHazardIngest: envOrDefault("KAFKA_TOPIC_HAZARD_INGEST", DefaultKafkaTopicHazardIngest),
		HealthPort:             envOrDefault("HEALTH_PORT", DefaultHealthPort),
	}

	if err := validate(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// validate checks whether the configuration is valid.
func validate(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("config must not be nil")
	}
	baseURL, err := url.Parse(strings.TrimSpace(cfg.SatelliteBaseURL))
	if err != nil || baseURL.Host == "" || (baseURL.Scheme != "http" && baseURL.Scheme != "https") {
		return fmt.Errorf("SATELLITE_BASE_URL must be an absolute http or https URL")
	}
	if strings.TrimSpace(cfg.SatelliteGroupToken) == "" {
		return fmt.Errorf("SATELLITE_GROUP_TOKEN must be configured")
	}
	if cfg.ReconnectDelay <= 0 {
		return fmt.Errorf("SATELLITE_RECONNECT_DELAY_SECONDS must be positive")
	}
	if cfg.MaxLateness <= 0 {
		return fmt.Errorf("MAX_LATENESS_MINUTES must be positive")
	}
	if strings.TrimSpace(cfg.SQLiteDBPath) == "" {
		return fmt.Errorf("SQLITE_DB_PATH must be configured")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}
	for _, broker := range cfg.KafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("KAFKA_BROKERS contains an empty broker address")
		}
	}
	if strings.TrimSpace(cfg.KafkaTopicHazardIngest) == "" {
		return fmt.Errorf("KAFKA_TOPIC_HAZARD_INGEST must be configured")
	}

	port, err := strconv.Atoi(cfg.HealthPort)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("HEALTH_PORT must be an integer between 1 and 65535")
	}

	return nil
}

// envOrDefault returns an environment variable
// or its fallback value.
func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

// requiredEnv retrieves a mandatory environment variable.
func requiredEnv(key string) (string, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return "", fmt.Errorf("%s must be configured", key)
	}
	return value, nil
}

// durationOrDefault parses a duration from environment variables
// or returns the fallback value.
func durationOrDefault(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}

	return time.Duration(parsed) * time.Second, nil
}

// csvEnvOrDefault parses comma-separated environment values
// or returns the fallback values.
func csvEnvOrDefault(key, fallback string) []string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		value = fallback
	}

	parts := strings.Split(value, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
