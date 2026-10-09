package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	_ "github.com/mattn/go-sqlite3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/google/uuid"
)

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
var version = 1
var outage = false

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
	version = 2
	w.WriteHeader(200)
	fmt.Fprint(w, "Schema Changed")
}
func doOutage(w http.ResponseWriter, r *http.Request) {
	outage = true
	w.WriteHeader(200)
	fmt.Fprint(w, "Outage happened")
}
func VolcanoReportHandler(w http.ResponseWriter, r *http.Request) {
	var reports []VolcanoReport
	since := r.URL.Query().Get("since")
	if since == "" {
		result := db.Find(&reports)

		if result.Error != nil {
			log.Fatal(result.Error)
		}
		body, _ := json.Marshal(reports)
		w.Write(body)
		return
	}
	parsedsince, err := time.Parse(time.RFC3339, since)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprint(w, err.Error())
		return
	}

	result := db.Where("reported_at >= ?", parsedsince).Find(&reports)

	if result.Error != nil {
		log.Fatal(result.Error)
	}
	body, _ := json.Marshal(reports)
	w.Write(body)
}
func outageMiddleware(outage *bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if *outage {
			http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
			return
		}

		next.ServeHTTP(w, r)
	})
}
func main() {
	initdb()
	go func() {
		for true {
			time.Sleep(positiveSecondsFromEnv("PVMBG_EVENT_GENERATION_INTERVAL_SECONDS", 2))
			GenerateEvent()
		}
	}()
	pvmbg := http.NewServeMux()
	pvmbg.HandleFunc("GET /volcanic-reports", VolcanoReportHandler)
	pvmbg.HandleFunc("GET /health", health)
	pvmbg.HandleFunc("POST /admin/schema-version", schemachange)
	pvmbg.HandleFunc("POST /admin/outage", doOutage)

	listenAddress := envOrDefault("PVMBG_LISTEN_ADDR", ":8082")
	fmt.Printf("Server is running on http://localhost%s\n", listenAddress)
	handler := outageMiddleware(&outage, apiKeyMiddleware(pvmbg))

	if err := http.ListenAndServe(listenAddress, handler); err != nil {
		fmt.Printf("Server failed to start: %v\n", err)
	}
}

func initdb() {
	db, _ = gorm.Open(sqlite.Open(envOrDefault("PVMBG_DB_PATH", "app.db")), &gorm.Config{})

	db.AutoMigrate(&VolcanoReport{})
}
func GenerateEvent() {
	id := uuid.New()
	report := VolcanoReport{}
	gofakeit.Struct(&report)
	report.ReportID = id.String()
	report.ReportedAt = time.Now()
	if version == 1 {
		report.ConfidenceLevel = 0
	}
	result := db.Create(&report)

	if result.Error != nil {
		log.Fatal(result.Error)
	}
}
