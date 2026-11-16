package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dispatcherv1 "webhook-dispatcher/api/proto/dispatcher/v1"
)

type EndpointResponse struct {
	ID        string `json:"id"`
	TenantID  string `json:"tenant_id"`
	TargetURL string `json:"target_url"`
	SecretKey string `json:"secret_key"`
}

func main() {
	grpcAddr := flag.String("grpc-addr", "localhost:50051", "Dispatcher gRPC server address")
	httpAddr := flag.String("http-addr", "http://localhost:8080", "Dispatcher HTTP API base URL")
	mockAddr := flag.String("mock-addr", "http://localhost:9090", "Mock Receiver base URL")
	secret := flag.String("secret", "mock-secret-key-12345", "Shared HMAC secret key")
	scenario := flag.String("scenario", "all", "Scenario to run: success, flaky, fail_dlq_replay, or all")
	flag.Parse()

	slog.Info("Starting Webhook Dispatcher End-to-End Demo",
		"grpc_addr", *grpcAddr,
		"http_addr", *httpAddr,
		"mock_addr", *mockAddr,
		"scenario", *scenario,
	)

	// 1. Check health of services
	resp, err := http.Get(*httpAddr + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		slog.Error("Dispatcher HTTP service not healthy or reachable", "error", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	slog.Info("Dispatcher HTTP service is HEALTHY")

	// 2. Setup gRPC connection
	conn, err := grpc.NewClient(*grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("Failed to connect to gRPC server", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	grpcClient := dispatcherv1.NewDispatcherServiceClient(conn)

	tenantID := "studio-epic-games"

	// Register test endpoints
	successEndpointID := registerEndpoint(*httpAddr, tenantID, "payment.success", *mockAddr+"/webhook/success", *secret)
	flakyEndpointID := registerEndpoint(*httpAddr, tenantID, "game.loot_drop", *mockAddr+"/webhook/flaky", *secret)
	failEndpointID := registerEndpoint(*httpAddr, tenantID, "order.refunded", *mockAddr+"/webhook/fail", *secret)

	time.Sleep(500 * time.Millisecond)

	if *scenario == "success" || *scenario == "all" {
		runSuccessScenario(grpcClient, *httpAddr, tenantID, successEndpointID)
	}

	if *scenario == "flaky" || *scenario == "all" {
		runFlakyScenario(grpcClient, *httpAddr, tenantID, flakyEndpointID)
	}

	if *scenario == "fail_dlq_replay" || *scenario == "all" {
		runDLQAndReplayScenario(grpcClient, *httpAddr, tenantID, failEndpointID)
	}

	slog.Info("All scenarios completed successfully!")
}

func registerEndpoint(httpAddr, tenantID, eventType, targetURL, secret string) string {
	payload, _ := json.Marshal(map[string]interface{}{
		"tenant_id":          tenantID,
		"event_type":         eventType,
		"target_url":         targetURL,
		"secret_key":         secret,
		"rate_limit_per_sec": 50,
	})

	resp, err := http.Post(httpAddr+"/api/v1/endpoints", "application/json", bytes.NewReader(payload))
	if err != nil {
		slog.Error("Failed to register endpoint", "url", targetURL, "error", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var ep EndpointResponse
	_ = json.NewDecoder(resp.Body).Decode(&ep)
	slog.Info("Registered Webhook Endpoint", "id", ep.ID, "event_type", eventType, "target_url", targetURL)
	return ep.ID
}

func runSuccessScenario(client dispatcherv1.DispatcherServiceClient, httpAddr, tenantID, epID string) {
	fmt.Println("\n==================================================")
	fmt.Println(" SCENARIO 1: Immediate Success Delivery")
	fmt.Println("==================================================")

	eventID := fmt.Sprintf("evt-succ-%d", time.Now().UnixNano()%100000)
	payload := []byte(fmt.Sprintf(`{"payment_id":"pay_98765","amount":49.99,"currency":"USD","player_id":"player_1"}`))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	slog.Info("Dispatching gRPC Event", "event_id", eventID, "tenant_id", tenantID, "type", "payment.success")
	res, err := client.DispatchEvent(ctx, &dispatcherv1.EventRequest{
		EventId:   eventID,
		TenantId:  tenantID,
		EventType: "payment.success",
		Payload:   payload,
	})
	if err != nil {
		slog.Error("gRPC DispatchEvent failed", "error", err)
		return
	}

	slog.Info("gRPC Response Received", "status", res.Status.String(), "message", res.Message)

	// Poll event status from HTTP API
	time.Sleep(500 * time.Millisecond)
	inspectEvent(httpAddr, eventID)
}

func runFlakyScenario(client dispatcherv1.DispatcherServiceClient, httpAddr, tenantID, epID string) {
	fmt.Println("\n==================================================")
	fmt.Println(" SCENARIO 2: Flaky Endpoint & Exponential Backoff")
	fmt.Println("==================================================")

	eventID := fmt.Sprintf("evt-flaky-%d", time.Now().UnixNano()%100000)
	payload := []byte(fmt.Sprintf(`{"drop_id":"item_legendary_sword","rarity":"mythic","player_id":"player_2"}`))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	slog.Info("Dispatching Flaky Event via gRPC", "event_id", eventID)
	_, err := client.DispatchEvent(ctx, &dispatcherv1.EventRequest{
		EventId:   eventID,
		TenantId:  tenantID,
		EventType: "game.loot_drop",
		Payload:   payload,
	})
	if err != nil {
		slog.Error("gRPC DispatchEvent failed", "error", err)
		return
	}

	// Inspect over 3 seconds to see retries
	for i := 1; i <= 3; i++ {
		time.Sleep(1 * time.Second)
		inspectEvent(httpAddr, eventID)
	}
}

func runDLQAndReplayScenario(client dispatcherv1.DispatcherServiceClient, httpAddr, tenantID, epID string) {
	fmt.Println("\n==================================================")
	fmt.Println(" SCENARIO 3: Permanent Failure, DLQ & Replay")
	fmt.Println("==================================================")

	eventID := fmt.Sprintf("evt-dlq-%d", time.Now().UnixNano()%100000)
	payload := []byte(fmt.Sprintf(`{"refund_id":"ref_54321","reason":"player_dispute","amount":19.99}`))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	slog.Info("Dispatching Always-Failing Event via gRPC", "event_id", eventID)
	_, err := client.DispatchEvent(ctx, &dispatcherv1.EventRequest{
		EventId:   eventID,
		TenantId:  tenantID,
		EventType: "order.refunded",
		Payload:   payload,
	})
	if err != nil {
		slog.Error("gRPC DispatchEvent failed", "error", err)
		return
	}

	time.Sleep(1 * time.Second)
	inspectEvent(httpAddr, eventID)

	// Fetch DLQ items
	dlqURL := httpAddr + "/api/v1/dlq"
	resp, err := http.Get(dlqURL)
	if err == nil {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("\n[DLQ Inspection] Current DLQ Items:\n%s\n", string(body))
	}

	// Demonstrate DLQ Replay endpoint
	replayURL := fmt.Sprintf("%s/api/v1/dlq/%s/replay", httpAddr, eventID)
	slog.Info("Triggering manual DLQ Replay", "url", replayURL)
	replayResp, err := http.Post(replayURL, "application/json", nil)
	if err != nil {
		slog.Error("Replay request failed", "error", err)
		return
	}
	defer replayResp.Body.Close()
	replayBody, _ := io.ReadAll(replayResp.Body)
	fmt.Printf("[DLQ Replay Response]: %s\n", string(replayBody))
}

func inspectEvent(httpAddr, eventID string) {
	resp, err := http.Get(fmt.Sprintf("%s/api/v1/events/%s", httpAddr, eventID))
	if err != nil {
		slog.Warn("Failed to inspect event", "event_id", eventID, "error", err)
		return
	}
	defer resp.Body.Close()

	var data map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&data)
	ev, _ := data["event"].(map[string]interface{})
	status := ev["status"]
	retries := ev["retry_count"]
	attempts, _ := data["attempts"].([]interface{})

	slog.Info("[Event Inspector]",
		"event_id", eventID,
		"status", status,
		"retry_count", retries,
		"recorded_attempts", len(attempts),
	)
}
