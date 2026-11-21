package main

import (
	"crypto/subtle"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	_ "github.com/mattn/go-sqlite3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/google/uuid"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

var volcanos = []string{
	"Sumbing",
	"Salak",
	"Gede",
	"Tangkuban Parahu",
	"Guntur",
	"Papandayan",
	"Galunggung",
	"Ciremai",
	"Slamet",
	"Dieng",
	"Sundoro",
	"Merapi",
	"Kelud",
	"Arjuno Welirang",
	"Bromo",
	"Semeru",
	"Lamongan",
	"Raung",
	"Ijen",
}

type VolcanoReport struct {
	ReportID         string    `json:"report_id" gorm:"primaryKey" `
	VolcanoID        string    `json:"volcano_id" fake:"{randomstring:volcanos}"`
	AlertLevel       string    `json:"alert_level" fake:"{randomstring:[Normal,Waspada,Siaga,Awas]}"`
	EruptionCount24h int       `json:"eruption_count_24h" fake:"{number:0,100}"`
	AshColumnHeightM float64   `json:"ash_column_height_m" fake:"{float64:0,5000}"`
	ReportedAt       time.Time `json:"reported_at"`
	ConfidenceLevel  float64   `json:"confidence_level,omitempty" fake:"{float64range:0.000001,1}"`
}

var db *gorm.DB
var stateMu sync.RWMutex
var schemaMode = "v1"
var outage = false
var pendingMalformed []string

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func positiveSecondsFromEnv(name string, fallback int) time.Duration {
	value := envOrDefault(name, strconv.Itoa(fallback))
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		log.Fatalf("%s must be a positive integer, got %q", name, value)
	}
	return time.Duration(seconds) * time.Second
}

func responseDelay() time.Duration {
	minimum, minErr := strconv.Atoi(envOrDefault("PVMBG_MIN_DELAY_MS", "500"))
	maximum, maxErr := strconv.Atoi(envOrDefault("PVMBG_MAX_DELAY_MS", "3000"))
	if minErr != nil || maxErr != nil || minimum < 500 || maximum > 3000 || minimum > maximum {
		log.Fatalf("PVMBG_MIN_DELAY_MS and PVMBG_MAX_DELAY_MS must be between 500 and 3000")
	}
	return time.Duration(minimum+rand.IntN(maximum-minimum+1)) * time.Millisecond
}

func requireEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s must be configured", name)
	}
	return value
}

func apiKeyMiddleware(next http.Handler) http.Handler {
	expectedKey := requireEnv("PVMBG_API_KEY")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}

		const prefix = "Bearer "
		authorization := r.Header.Get("Authorization")
		if !strings.HasPrefix(authorization, prefix) {
			http.Error(w, "missing bearer API key", http.StatusUnauthorized)
			return
		}
		providedKey := strings.TrimPrefix(authorization, prefix)
		if subtle.ConstantTimeCompare([]byte(providedKey), []byte(expectedKey)) != 1 {
			http.Error(w, "invalid PVMBG credentials", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	fmt.Fprint(w, "UP")
}
func schemachange(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mode string `json:"mode"`
	}
	if r.Body != nil {
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&req); err != nil && err.Error() != "EOF" {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
	}
	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = "additive"
	} else if mode == "v2" {
		mode = "additive"
	}
	switch mode {
	case "v1", "additive", "breaking_rename", "breaking_type", "breaking_missing":
	default:
		http.Error(w, "unsupported schema mode", http.StatusBadRequest)
		return
	}

	stateMu.Lock()
	schemaMode = mode
	stateMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"mode": mode})
}

func injectMalformed(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mode string `json:"mode"`
		Type string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	kind := strings.TrimSpace(req.Mode)
	if kind == "" {
		kind = strings.TrimSpace(req.Type)
	}
	switch kind {
	case "truncated", "deep_nesting", "invalid_utf8", "oversized":
	default:
		http.Error(w, "unsupported malformed type; expected truncated, deep_nesting, invalid_utf8, or oversized", http.StatusBadRequest)
		return
	}

	stateMu.Lock()
	pendingMalformed = append(pendingMalformed, kind)
	stateMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"queued": kind})
}

func doOutage(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "request body must be {\"enabled\": true|false}", http.StatusBadRequest)
		return
	}
	stateMu.Lock()
	outage = request.Enabled
	current := outage
	stateMu.Unlock()
	w.WriteHeader(200)
	fmt.Fprintf(w, "outage=%t", current)
}

func formatReportForMode(report VolcanoReport, mode string) map[string]any {
	base := map[string]any{
		"report_id":            report.ReportID,
		"volcano_id":           report.VolcanoID,
		"alert_level":          report.AlertLevel,
		"eruption_count_24h":   report.EruptionCount24h,
		"ash_column_height_m":  report.AshColumnHeightM,
		"reported_at":          report.ReportedAt,
	}
	switch mode {
	case "additive":
		if report.ConfidenceLevel != 0 {
			base["confidence_level"] = report.ConfidenceLevel
		} else {
			base["confidence_level"] = 0.95
		}
	case "breaking_rename":
		delete(base, "alert_level")
		delete(base, "volcano_id")
		base["status_tingkat"] = report.AlertLevel
		base["mountain_name"] = report.VolcanoID
	case "breaking_type":
		base["eruption_count_24h"] = fmt.Sprintf("%d kali", report.EruptionCount24h)
		base["alert_level"] = 3
	case "breaking_missing":
		delete(base, "volcano_id")
	}
	return base
}

func buildMalformedPayload(kind string, validReports []map[string]any) []byte {
	switch kind {
	case "truncated":
		return []byte(`[{"report_id":"malformed-truncated-1","volcano_id":"Merapi","alert_level":"Siaga"`)
	case "invalid_utf8":
		return append([]byte(`[{"report_id":"malformed-utf8-1","volcano_id":"`), []byte{0xff, 0xfe, 0xfd, '"', '}'}...)
	case "deep_nesting":
		var b strings.Builder
		for i := 0; i < 70; i++ {
			b.WriteString(`{"n":`)
		}
		b.WriteString(`1`)
		for i := 0; i < 70; i++ {
			b.WriteString(`}`)
		}
		bad := map[string]any{
			"report_id":           "malformed-deep-1",
			"volcano_id":          "Merapi",
			"alert_level":         "Siaga",
			"eruption_count_24h":  1,
			"ash_column_height_m": 100.0,
			"reported_at":         time.Now().UTC(),
			"nested":              json.RawMessage(b.String()),
		}
		items := append(validReports, bad)
		out, _ := json.Marshal(items)
		return out
	case "oversized":
		bigStr := strings.Repeat("A", 2*1024*1024+1024)
		bad := map[string]any{
			"report_id":           "malformed-oversized-1",
			"volcano_id":          "Merapi",
			"alert_level":         "Siaga",
			"eruption_count_24h":  1,
			"ash_column_height_m": 100.0,
			"reported_at":         time.Now().UTC(),
			"padding":             bigStr,
		}
		items := append(validReports, bad)
		out, _ := json.Marshal(items)
		return out
	default:
		out, _ := json.Marshal(validReports)
		return out
	}
}

func VolcanoReportHandler(w http.ResponseWriter, r *http.Request) {
	var reports []VolcanoReport
	since := r.URL.Query().Get("since")
	if since == "" {
		result := db.Find(&reports)
		if result.Error != nil {
			log.Printf("query volcanic reports: %v", result.Error)
			http.Error(w, "temporary database error", http.StatusInternalServerError)
			return
		}
	} else {
		parsedsince, err := time.Parse(time.RFC3339, since)
		if err != nil {
			w.WriteHeader(400)
			fmt.Fprint(w, err.Error())
			return
		}
		result := db.Where("reported_at >= ?", parsedsince).Find(&reports)
		if result.Error != nil {
			log.Printf("query volcanic reports since %s: %v", parsedsince.Format(time.RFC3339), result.Error)
			http.Error(w, "temporary database error", http.StatusInternalServerError)
			return
		}
	}

	stateMu.Lock()
	mode := schemaMode
	var malformedKind string
	if len(pendingMalformed) > 0 {
		malformedKind = pendingMalformed[0]
		pendingMalformed = pendingMalformed[1:]
	}
	stateMu.Unlock()

	formatted := make([]map[string]any, 0, len(reports))
	for _, rep := range reports {
		formatted = append(formatted, formatReportForMode(rep, mode))
	}

	w.Header().Set("Content-Type", "application/json")
	if malformedKind != "" {
		w.Write(buildMalformedPayload(malformedKind, formatted))
		return
	}
	body, _ := json.Marshal(formatted)
	w.Write(body)
}

func outageMiddleware(_ *bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/outage" || r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		stateMu.RLock()
		isOutage := outage
		stateMu.RUnlock()
		if isOutage {
			http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
			return
		}
		time.Sleep(responseDelay())
		next.ServeHTTP(w, r)
	})
}
func main() {
	initdb()
	go func() {
		for true {
			time.Sleep(positiveSecondsFromEnv("PVMBG_EVENT_GENERATION_INTERVAL_SECONDS", 2))
			if err := GenerateEvent(); err != nil {
				log.Printf("generate PVMBG event: %v", err)
			}
		}
	}()
	pvmbg := http.NewServeMux()
	pvmbg.HandleFunc("GET /volcanic-reports", VolcanoReportHandler)
	pvmbg.HandleFunc("GET /health", health)
	pvmbg.HandleFunc("POST /admin/schema-version", schemachange)
	pvmbg.HandleFunc("POST /admin/inject-malformed", injectMalformed)
	pvmbg.HandleFunc("POST /admin/outage", doOutage)

	listenAddress := envOrDefault("PVMBG_LISTEN_ADDR", ":8082")
	fmt.Printf("Server is running on http://localhost%s\n", listenAddress)
	handler := upstreamLogMiddleware(outageMiddleware(&outage, apiKeyMiddleware(pvmbg)))

	if err := http.ListenAndServe(listenAddress, handler); err != nil {
		fmt.Printf("Server failed to start: %v\n", err)
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func upstreamLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/volcanic-reports" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		entry, _ := json.Marshal(map[string]any{
			"time":            time.Now().UTC().Format(time.RFC3339Nano),
			"method":          r.Method,
			"path":            r.URL.Path,
			"query":           r.URL.RawQuery,
			"poller_instance": r.Header.Get("X-Poller-Instance"),
			"fencing_token":   r.Header.Get("X-Fencing-Token"),
			"correlation_id":  r.Header.Get("X-Correlation-ID"),
			"status":          rec.status,
			"duration_ms":     time.Since(start).Milliseconds(),
		})
		fmt.Println(string(entry))
	})
}

func initdb() {
	databasePath := envOrDefault("PVMBG_DB_PATH", "app.db")
	rawDB, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		log.Fatalf("opening PVMBG database: %v", err)
	}
	if err := runMigrations(rawDB); err != nil {
		_ = rawDB.Close()
		log.Fatalf("applying PVMBG migrations: %v", err)
	}
	if err := rawDB.Close(); err != nil {
		log.Fatalf("closing PVMBG migration connection: %v", err)
	}
	db, err = gorm.Open(sqlite.Open(databasePath), &gorm.Config{})
	if err != nil {
		log.Fatalf("opening PVMBG GORM connection: %v", err)
	}
}

func runMigrations(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY)`); err != nil {
		return err
	}
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var applied int
		if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE name = ?`, entry.Name()).Scan(&applied); err != nil {
			return err
		}
		if applied > 0 {
			continue
		}
		script, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}
		transaction, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err = transaction.Exec(string(script)); err == nil {
			_, err = transaction.Exec(`INSERT INTO schema_migrations(name) VALUES(?)`, entry.Name())
		}
		if err != nil {
			_ = transaction.Rollback()
			return fmt.Errorf("%s: %w", entry.Name(), err)
		}
		if err := transaction.Commit(); err != nil {
			return err
		}
	}
	return nil
}
func GenerateEvent() error {
	return generateEventAt(time.Now())
}

func generateEventAt(reportedAt time.Time) error {
	id := uuid.New()
	report := VolcanoReport{}
	gofakeit.Struct(&report)
	report.ReportID = id.String()
	report.VolcanoID = gofakeit.RandomString(volcanos)
	report.ReportedAt = reportedAt
	stateMu.RLock()
	currentMode := schemaMode
	stateMu.RUnlock()
	if currentMode == "v1" {
		report.ConfidenceLevel = 0
	}
	result := db.Create(&report)

	if result.Error != nil {
		return result.Error
	}
	return nil
}
