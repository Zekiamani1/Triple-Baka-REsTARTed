package main

import (
	"crypto/subtle"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

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
	milliseconds, err := strconv.Atoi(envOrDefault("BMKG_RESPONSE_DELAY_MS", "100"))
	if err != nil || milliseconds < 50 || milliseconds > 150 {
		log.Fatalf("BMKG_RESPONSE_DELAY_MS must be between 50 and 150, got %q", os.Getenv("BMKG_RESPONSE_DELAY_MS"))
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func databasePath() string {
	return envOrDefault("BMKG_DB_PATH", "./bmkg.db")
}

func requireEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s must be configured", name)
	}
	return value
}

func apiKeyMiddleware(next http.Handler) http.Handler {
	expectedKey := requireEnv("BMKG_API_KEY")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}

		providedKey := r.Header.Get("X-BMKG-Key")
		if providedKey == "" {
			http.Error(w, "missing X-BMKG-Key", http.StatusUnauthorized)
			return
		}
		if subtle.ConstantTimeCompare([]byte(providedKey), []byte(expectedKey)) != 1 {
			http.Error(w, "invalid BMKG credentials", http.StatusForbidden)
			return
		}
		time.Sleep(responseDelay())
		next.ServeHTTP(w, r)
	})
}

type SeismicEvent struct {
	EventID          string    `json:"event_id"`
	Magnitude        float64   `json:"magnitude"`
	DepthKM          float64   `json:"depth_km"`
	EpicenterLat     float64   `json:"epicenter_lat"`
	EpicenterLon     float64   `json:"epicenter_lon"`
	RegionName       string    `json:"region_name"`
	OccurredAt       time.Time `json:"occurred_at"`
	PotentialTsunami bool      `json:"potential_tsunami"`
}
type TsunamiWarning struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"`
	AffectedZones    string    `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
}

func health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	fmt.Fprint(w, "UP")
}
func seismicHandler(w http.ResponseWriter, r *http.Request) {
	since := r.URL.Query().Get("since")
	if since == "" {
		event := fetchallevents()
		body, _ := json.Marshal(event)
		w.Write(body)
		return
	}
	parsedsince, err := time.Parse(time.RFC3339, since)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprint(w, err.Error())
		return
	}
	event := fetcheventbytime(parsedsince)
	body, _ := json.Marshal(event)
	w.Write(body)
}
func tsunamiHandler(w http.ResponseWriter, r *http.Request) {
	since := r.URL.Query().Get("since")
	if since == "" {
		event := fetchalltsunami()
		body, _ := json.Marshal(event)
		w.Write(body)
		return
	}
	parsedsince, err := time.Parse(time.RFC3339, since)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprint(w, err.Error())
		return
	}
	event := fetchtsunamibytime(parsedsince)
	body, _ := json.Marshal(event)
	w.Write(body)
}
func initdb() {
	db, err := sql.Open("sqlite3", databasePath())
	if err != nil {
		log.Fatalf("Gagal membuka koneksi database: %v", err)
	}
	defer db.Close()
	if err := runMigrations(db); err != nil {
		log.Fatalf("applying BMKG migrations: %v", err)
	}
}

// SQLite may return timestamps with a space separator and either fractional or
// whole seconds. Z07:00 is a timezone placeholder; +07:00 would be literal.
func parseDatabaseTimestamp(value string) (time.Time, error) {
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05Z07:00",
		time.RFC3339Nano,
	} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported database timestamp %q", value)
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
func fetchallevents() []SeismicEvent {
	db, _ := sql.Open("sqlite3", databasePath())
	defer db.Close()
	rows, err := db.Query(`
	SELECT *
	FROM seismic_event
	ORDER BY occurred_at ASC;`)
	if err != nil {
		log.Printf(err.Error())
		return nil
	}
	var events []SeismicEvent
	for rows.Next() {
		var seismic SeismicEvent
		var time1 string
		err = rows.Scan(&seismic.EventID, &seismic.Magnitude,
			&seismic.DepthKM, &seismic.EpicenterLat, &seismic.EpicenterLon,
			&seismic.RegionName, &time1, &seismic.PotentialTsunami)
		if err != nil {
			log.Printf("scan seismic event: %v", err)
			return nil
		}
		seismic.OccurredAt, err = parseDatabaseTimestamp(time1)
		if err != nil {
			log.Printf("parse seismic event time: %v", err)
			return nil
		}
		events = append(events, seismic)
	}
	return events
}
func fetcheventbytime(since time.Time) []SeismicEvent {
	db, _ := sql.Open("sqlite3", databasePath())
	defer db.Close()
	stmt, err := db.Prepare(`
	SELECT *
	FROM seismic_event
	WHERE occurred_at >= ?
	ORDER BY occurred_at ASC;`)
	if err != nil {
		log.Printf(err.Error())
		return nil
	}

	rows, err := stmt.Query(since)
	if err != nil {
		log.Printf(err.Error())
		return nil
	}
	var events []SeismicEvent
	for rows.Next() {
		var seismic SeismicEvent
		var time1 string
		err = rows.Scan(&seismic.EventID, &seismic.Magnitude,
			&seismic.DepthKM, &seismic.EpicenterLat, &seismic.EpicenterLon,
			&seismic.RegionName, &time1, &seismic.PotentialTsunami)
		if err != nil {
			log.Printf("scan seismic event: %v", err)
			return nil
		}
		seismic.OccurredAt, err = parseDatabaseTimestamp(time1)
		if err != nil {
			log.Printf("parse seismic event time: %v", err)
			return nil
		}
		events = append(events, seismic)
	}
	return events
}

func fetchtsunamibytime(since time.Time) []TsunamiWarning {
	db, _ := sql.Open("sqlite3", databasePath())
	defer db.Close()
	stmt, err := db.Prepare(`
	SELECT warning_id, related_event_id, threat_level, affected_zones, estimated_arrival
	FROM seismic_event as s
	JOIN tsunami_warnings as ts
	on ts.related_event_id = s.event_id
	WHERE s.occurred_at >= ?
	ORDER BY s.occurred_at ASC;`)
	if err != nil {
		log.Printf(err.Error())
		return nil
	}

	rows, err := stmt.Query(since)
	if err != nil {
		log.Printf(err.Error())
		return nil
	}
	var events []TsunamiWarning
	for rows.Next() {
		var tsunami TsunamiWarning
		var time1 string
		err = rows.Scan(&tsunami.WarningID, &tsunami.RelatedEventID, &tsunami.ThreatLevel, &tsunami.AffectedZones, &time1)
		if err != nil {
			log.Printf("scan tsunami warning: %v", err)
			return nil
		}
		tsunami.EstimatedArrival, err = parseDatabaseTimestamp(time1)
		if err != nil {
			log.Printf("parse tsunami warning time: %v", err)
			return nil
		}
		events = append(events, tsunami)
	}
	return events
}

func fetchalltsunami() []TsunamiWarning {
	db, _ := sql.Open("sqlite3", databasePath())
	defer db.Close()
	rows, err := db.Query(`
	SELECT warning_id, related_event_id, threat_level, affected_zones, estimated_arrival
	FROM seismic_event as s
	JOIN tsunami_warnings as ts
	on ts.related_event_id = s.event_id
	ORDER BY s.occurred_at ASC;`)
	if err != nil {
		log.Printf(err.Error())
		return nil
	}
	var events []TsunamiWarning
	for rows.Next() {
		var tsunami TsunamiWarning
		var time1 string
		err = rows.Scan(&tsunami.WarningID, &tsunami.RelatedEventID, &tsunami.ThreatLevel, &tsunami.AffectedZones, &time1)
		if err != nil {
			log.Printf("scan tsunami warning: %v", err)
			return nil
		}
		tsunami.EstimatedArrival, err = parseDatabaseTimestamp(time1)
		if err != nil {
			log.Printf("parse tsunami warning time: %v", err)
			return nil
		}
		events = append(events, tsunami)
	}
	return events
}

func main() {
	initdb()
	go func() {
		for true {
			time.Sleep(positiveSecondsFromEnv("BMKG_EVENT_GENERATION_INTERVAL_SECONDS", 2))
			if err := GenerateEvent(); err != nil {
				log.Printf("generate BMKG event: %v", err)
			}
		}
	}()
	bmkg := http.NewServeMux()
	bmkg.HandleFunc("GET /seismic-events", seismicHandler)
	bmkg.HandleFunc("GET /tsunami-warnings", tsunamiHandler)
	bmkg.HandleFunc("GET /health", health)

	listenAddress := envOrDefault("BMKG_LISTEN_ADDR", ":8081")
	fmt.Printf("Server is running on http://localhost%s\n", listenAddress)

	if err := http.ListenAndServe(listenAddress, apiKeyMiddleware(bmkg)); err != nil {
		fmt.Printf("Server failed to start: %v\n", err)
	}
}

var threatLevels = []string{
	"Waspada",
	"Siaga",
	"Awas",
}

func GenerateEvent() error {
	return generateEventAt(time.Now())
}

func generateEventAt(occurredAt time.Time) error {
	id := uuid.New()
	seismic := SeismicEvent{
		EventID:          id.String(),
		Magnitude:        gofakeit.Float64Range(4.0, 8.0),
		DepthKM:          gofakeit.Float64Range(1.0, 300.0),
		EpicenterLat:     gofakeit.Latitude(),
		EpicenterLon:     gofakeit.Longitude(),
		RegionName:       gofakeit.City(),
		OccurredAt:       occurredAt,
		PotentialTsunami: gofakeit.Bool(),
	}
	db, _ := sql.Open("sqlite3", databasePath())
	defer db.Close()

	stmt, err := db.Prepare(`
	INSERT INTO seismic_event (
				event_id,
				magnitude,
				depth_km,
				epicenter_lat,
				epicenter_lon,
				region_name,
				occurred_at,
				potential_tsunami
	)VALUES (?,?,?,?,?,?,?,?);`)
	if err != nil {
		return fmt.Errorf("prepare seismic insert: %w", err)
	}
	defer stmt.Close()
	_, err = stmt.Exec(seismic.EventID, seismic.Magnitude,
		seismic.DepthKM, seismic.EpicenterLat, seismic.EpicenterLon,
		seismic.RegionName, seismic.OccurredAt, seismic.PotentialTsunami)
	if err != nil {
		return fmt.Errorf("insert seismic event: %w", err)
	}

	if !seismic.PotentialTsunami {
		return nil
	}
	tsunamiid := uuid.New()
	tsunami := TsunamiWarning{
		WarningID:      tsunamiid.String(),
		RelatedEventID: id.String(),
		ThreatLevel:    gofakeit.RandomString(threatLevels),
		AffectedZones:  gofakeit.City(),
		EstimatedArrival: occurredAt.Add(
			time.Duration(gofakeit.IntRange(10, 180)) * time.Minute,
		),
	}
	stmt2, err := db.Prepare(`
	INSERT INTO tsunami_warnings (
		warning_id,
		related_event_id,
		threat_level,
		affected_zones,
		estimated_arrival
	) VALUES (?,?,?,?,?);`)
	if err != nil {
		return fmt.Errorf("prepare tsunami insert: %w", err)
	}
	defer stmt2.Close()

	_, err = stmt2.Exec(tsunami.WarningID, tsunami.RelatedEventID, tsunami.ThreatLevel, tsunami.AffectedZones, tsunami.EstimatedArrival)
	if err != nil {
		return fmt.Errorf("insert tsunami warning: %w", err)
	}
	return nil
}
