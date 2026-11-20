package main

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config contains the runtime configuration for the Coastal Sensor Adapter.
// Values are read from environment variables so Kubernetes can supply them
// through ConfigMaps and Secrets.
type Config struct {
	SensorBaseURL              string
	PantaiAuthToken            string
	PollInterval               time.Duration
	HTTPTimeout                time.Duration
	BreakerFailureThreshold    int
	BreakerOpenDuration        time.Duration
	KafkaBrokers               []string
	KafkaTopicPantaiIngest     string
	KafkaTopicPantaiQuarantine string
	PantaiStateURI             string
	HealthPort                 int
	OTelServiceName            string
	OTelExporterOTLPEndpoint   string
}

// LoadConfig reads, parses, and validates the adapter configuration.
// Defaults match the initial values in the Problem 5 guide. Credentials and
// the durable state-store URI are required and intentionally have no defaults.
func LoadConfig() (Config, error) {
	var cfg Config
	var err error

	cfg.SensorBaseURL = envOrDefault("SENSOR_BASE_URL", "http://sensor-pantai:8080")
	cfg.SensorBaseURL, err = validateHTTPURL("SENSOR_BASE_URL", cfg.SensorBaseURL)
	if err != nil {
		return Config{}, err
	}

	cfg.PantaiAuthToken, err = requiredEnv("PANTAI_AUTH_TOKEN")
	if err != nil {
		return Config{}, err
	}

	pollIntervalMS, err := positiveIntEnv("POLL_INTERVAL_MS", 1000)
	if err != nil {
		return Config{}, err
	}
	cfg.PollInterval = time.Duration(pollIntervalMS) * time.Millisecond

	httpTimeoutMS, err := positiveIntEnv("HTTP_TIMEOUT_MS", 1000)
	if err != nil {
		return Config{}, err
	}
	cfg.HTTPTimeout = time.Duration(httpTimeoutMS) * time.Millisecond

	cfg.BreakerFailureThreshold, err = positiveIntEnv("BREAKER_FAILURE_THRESHOLD", 3)
	if err != nil {
		return Config{}, err
	}

	breakerOpenMS, err := positiveIntEnv("BREAKER_OPEN_MS", 10000)
	if err != nil {
		return Config{}, err
	}
	cfg.BreakerOpenDuration = time.Duration(breakerOpenMS) * time.Millisecond

	brokersValue := envOrDefault("KAFKA_BROKERS", "kafka-aggregate:29092")
	cfg.KafkaBrokers, err = parseKafkaBrokers(brokersValue)
	if err != nil {
		return Config{}, err
	}

	cfg.KafkaTopicPantaiIngest = envOrDefault(
		"KAFKA_TOPIC_PANTAI_INGEST",
		"pantai-ingest",
	)
	if err := validateKafkaTopic("KAFKA_TOPIC_PANTAI_INGEST", cfg.KafkaTopicPantaiIngest); err != nil {
		return Config{}, err
	}

	cfg.KafkaTopicPantaiQuarantine = envOrDefault(
		"KAFKA_TOPIC_PANTAI_QUARANTINE",
		"pantai-quarantine",
	)
	if err := validateKafkaTopic("KAFKA_TOPIC_PANTAI_QUARANTINE", cfg.KafkaTopicPantaiQuarantine); err != nil {
		return Config{}, err
	}
	if cfg.KafkaTopicPantaiIngest == cfg.KafkaTopicPantaiQuarantine {
		return Config{}, fmt.Errorf("KAFKA_TOPIC_PANTAI_INGEST and KAFKA_TOPIC_PANTAI_QUARANTINE must be different")
	}

	cfg.PantaiStateURI, err = requiredEnv("PANTAI_STATE_URI")
	if err != nil {
		return Config{}, err
	}

	cfg.HealthPort, err = portEnv("HEALTH_PORT", 8089)
	if err != nil {
		return Config{}, err
	}

	cfg.OTelServiceName = envOrDefault("OTEL_SERVICE_NAME", "sensor-pantai-adapter")
	if strings.TrimSpace(cfg.OTelServiceName) == "" {
		return Config{}, fmt.Errorf("OTEL_SERVICE_NAME must not be empty")
	}

	cfg.OTelExporterOTLPEndpoint = envOrDefault(
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"http://otel-lgtm:4317",
	)
	if cfg.OTelExporterOTLPEndpoint != "" {
		cfg.OTelExporterOTLPEndpoint, err = validateHTTPURL(
			"OTEL_EXPORTER_OTLP_ENDPOINT",
			cfg.OTelExporterOTLPEndpoint,
		)
		if err != nil {
			return Config{}, err
		}
	}

	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	value, ok := lookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func requiredEnv(key string) (string, error) {
	value, ok := lookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("required environment variable %s is not set", key)
	}
	return strings.TrimSpace(value), nil
}

// lookupEnv is a small indirection to keep environment access in one place.
// It is a variable so tests in this package can replace it when needed.
var lookupEnv = func(key string) (string, bool) {
	return os.LookupEnv(key)
}

func positiveIntEnv(key string, fallback int) (int, error) {
	value := envOrDefault(key, strconv.Itoa(fallback))
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be greater than zero", key)
	}
	return parsed, nil
}

func portEnv(key string, fallback int) (int, error) {
	port, err := positiveIntEnv(key, fallback)
	if err != nil {
		return 0, err
	}
	if port > 65535 {
		return 0, fmt.Errorf("%s must be between 1 and 65535", key)
	}
	return port, nil
}

func validateHTTPURL(key, raw string) (string, error) {
	parsed, err := url.ParseRequestURI(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("%s must be a valid HTTP(S) URL: %w", key, err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("%s must include an http:// or https:// scheme and a host", key)
	}
	if parsed.User != nil {
		return "", fmt.Errorf("%s must not contain URL user information; use a Secret for credentials", key)
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func parseKafkaBrokers(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))

	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker == "" {
			return nil, fmt.Errorf("KAFKA_BROKERS contains an empty broker address")
		}
		host, portText, err := net.SplitHostPort(broker)
		if err != nil {
			return nil, fmt.Errorf("invalid Kafka broker address %q: expected host:port: %w", broker, err)
		}
		if host == "" {
			return nil, fmt.Errorf("invalid Kafka broker address %q: host must not be empty", broker)
		}
		port, err := strconv.Atoi(portText)
		if err != nil || port < 1 || port > 65535 {
			return nil, fmt.Errorf("invalid Kafka broker address %q: port must be between 1 and 65535", broker)
		}
		if _, exists := seen[broker]; exists {
			continue
		}
		seen[broker] = struct{}{}
		brokers = append(brokers, broker)
	}

	if len(brokers) == 0 {
		return nil, fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}
	return brokers, nil
}

func validateKafkaTopic(key, topic string) error {
	if topic == "" || len(topic) > 249 {
		return fmt.Errorf("%s must contain between 1 and 249 characters", key)
	}
	if topic == "." || topic == ".." {
		return fmt.Errorf("%s cannot be %q", key, topic)
	}
	for _, r := range topic {
		if !(r >= 'a' && r <= 'z') &&
			!(r >= 'A' && r <= 'Z') &&
			!(r >= '0' && r <= '9') &&
			r != '.' && r != '_' && r != '-' {
			return fmt.Errorf("%s contains an invalid character %q", key, r)
		}
	}
	return nil
}
