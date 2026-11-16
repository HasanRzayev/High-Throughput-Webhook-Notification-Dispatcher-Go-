package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"webhook-dispatcher/internal/security"
)

type EventStat struct {
	EventID       string    `json:"event_id"`
	Attempts      int       `json:"attempts"`
	LastReceived  time.Time `json:"last_received"`
	ValidSigCount int       `json:"valid_sig_count"`
	LastStatus    int       `json:"last_status"`
}

type ReceiverServer struct {
	port       string
	secretKey  string
	failRate   float64
	alwaysFail bool

	mu    sync.Mutex
	stats map[string]*EventStat
}

func main() {
	port := flag.String("port", "9090", "Port to listen on")
	secret := flag.String("secret", "mock-secret-key-12345", "Shared secret for HMAC verification")
	failRate := flag.Float64("fail-rate", 0.6, "Probability of failure (0.0 to 1.0)")
	alwaysFail := flag.Bool("always-fail", false, "Force 100% failure to test DLQ movement")
	flag.Parse()

	srv := &ReceiverServer{
		port:       *port,
		secretKey:  *secret,
		failRate:   *failRate,
		alwaysFail: *alwaysFail,
		stats:      make(map[string]*EventStat),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", srv.handleWebhook)
	mux.HandleFunc("/webhook/flaky", srv.handleFlakyWebhook)
	mux.HandleFunc("/webhook/fail", srv.handleAlwaysFailWebhook)
	mux.HandleFunc("/webhook/success", srv.handleAlwaysSuccessWebhook)
	mux.HandleFunc("/stats", srv.handleStats)

	addr := ":" + *port
	slog.Info("Starting Mock Game Studio Receiver",
		"addr", addr,
		"shared_secret", *secret,
		"default_fail_rate", *failRate,
		"always_fail", *alwaysFail,
	)

	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("Mock receiver failed", "error", err)
		os.Exit(1)
	}
}

func (s *ReceiverServer) handleWebhook(w http.ResponseWriter, r *http.Request) {
	s.processWebhook(w, r, s.failRate)
}

func (s *ReceiverServer) handleFlakyWebhook(w http.ResponseWriter, r *http.Request) {
	s.processWebhook(w, r, 0.5)
}

func (s *ReceiverServer) handleAlwaysFailWebhook(w http.ResponseWriter, r *http.Request) {
	s.processWebhook(w, r, 1.0)
}

func (s *ReceiverServer) handleAlwaysSuccessWebhook(w http.ResponseWriter, r *http.Request) {
	s.processWebhook(w, r, 0.0)
}

func (s *ReceiverServer) processWebhook(w http.ResponseWriter, r *http.Request, failProbability float64) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	eventID := r.Header.Get("X-Event-ID")
	sig := r.Header.Get("X-Webhook-Signature")
	tenantID := r.Header.Get("X-Tenant-ID")
	timestamp := r.Header.Get("X-Webhook-Timestamp")

	// HMAC-SHA256 signature verification
	isSigValid := security.VerifySignature(s.secretKey, body, sig)

	s.mu.Lock()
	stat, exists := s.stats[eventID]
	if !exists {
		stat = &EventStat{EventID: eventID}
		s.stats[eventID] = stat
	}
	stat.Attempts++
	stat.LastReceived = time.Now()
	if isSigValid {
		stat.ValidSigCount++
	}
	attemptNum := stat.Attempts
	s.mu.Unlock()

	slog.Info("--> Received Webhook",
		"event_id", eventID,
		"attempt", attemptNum,
		"tenant_id", tenantID,
		"timestamp", timestamp,
		"signature_valid", isSigValid,
		"signature", sig,
		"payload_size", len(body),
	)

	// Determine if this attempt should fail
	shouldFail := s.alwaysFail || (failProbability > 0 && rand.Float64() < failProbability)

	if shouldFail {
		s.mu.Lock()
		stat.LastStatus = http.StatusInternalServerError
		s.mu.Unlock()

		slog.Warn("Simulating flakiness: returning 500 Internal Server Error", "event_id", eventID, "attempt", attemptNum)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"simulated failure","attempt":%d}`, attemptNum)))
		return
	}

	s.mu.Lock()
	stat.LastStatus = http.StatusOK
	s.mu.Unlock()

	slog.Info("Successfully acknowledged webhook: returning 200 OK", "event_id", eventID, "attempt", attemptNum)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"ack","event_id":"%s","attempt":%d}`, eventID, attemptNum)))
}

func (s *ReceiverServer) handleStats(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.stats)
}
