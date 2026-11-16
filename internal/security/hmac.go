package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strings"
)

// SignPayload generates an HMAC-SHA256 signature for payload using secret key.
// Returns a hex-encoded string formatted as "sha256=<hex>".
func SignPayload(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	sum := mac.Sum(nil)
	return "sha256=" + hex.EncodeToString(sum)
}

// VerifySignature validates that signature matches HMAC-SHA256 of payload with secret.
// Protects against timing attacks using subtle.ConstantTimeCompare.
func VerifySignature(secret string, payload []byte, signature string) bool {
	cleanSig := strings.TrimSpace(signature)
	cleanSig = strings.TrimPrefix(cleanSig, "sha256=")

	expectedSig := SignPayload(secret, payload)
	expectedClean := strings.TrimPrefix(expectedSig, "sha256=")

	sigBytes, err1 := hex.DecodeString(cleanSig)
	expBytes, err2 := hex.DecodeString(expectedClean)
	if err1 != nil || err2 != nil {
		return false
	}

	return subtle.ConstantTimeCompare(sigBytes, expBytes) == 1
}
