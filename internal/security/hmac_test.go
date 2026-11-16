package security

import (
	"testing"
)

func TestSignPayload(t *testing.T) {
	secret := "secret-key-12345"
	payload := []byte(`{"event_id":"evt-1","amount":100}`)

	sig1 := SignPayload(secret, payload)
	sig2 := SignPayload(secret, payload)

	if sig1 == "" {
		t.Fatal("expected non-empty signature")
	}

	if sig1 != sig2 {
		t.Fatalf("expected deterministic signature, got %s and %s", sig1, sig2)
	}

	if len(sig1) <= 7 || sig1[:7] != "sha256=" {
		t.Fatalf("expected signature to start with 'sha256=', got %s", sig1)
	}
}

func TestVerifySignature(t *testing.T) {
	secret := "super-secure-studio-secret"
	payload := []byte(`{"player_id":"usr-999","event":"item_unlocked"}`)

	sig := SignPayload(secret, payload)

	// Valid signature with sha256= prefix
	if !VerifySignature(secret, payload, sig) {
		t.Fatal("expected signature to verify successfully")
	}

	// Valid signature without sha256= prefix
	rawHex := sig[7:]
	if !VerifySignature(secret, payload, rawHex) {
		t.Fatal("expected raw hex signature without prefix to verify successfully")
	}

	// Tampered payload
	tamperedPayload := []byte(`{"player_id":"usr-999","event":"item_unlocked_tampered"}`)
	if VerifySignature(secret, tamperedPayload, sig) {
		t.Fatal("expected tampered payload verification to fail")
	}

	// Invalid secret
	if VerifySignature("wrong-secret", payload, sig) {
		t.Fatal("expected verification with wrong secret to fail")
	}

	// Corrupted hex string
	if VerifySignature(secret, payload, "not-a-hex-signature") {
		t.Fatal("expected invalid hex signature to fail")
	}
}
