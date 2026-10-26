// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTokenMinterSignsAndCaches(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "jwt.key")
	key := []byte("0123456789abcdef0123456789abcdef")
	if err := os.WriteFile(keyFile, key, 0o600); err != nil {
		t.Fatal(err)
	}
	m := &tokenMinter{keyFile: keyFile, user: "slurmkube", lifetime: 10 * time.Minute}
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	tok, err := m.Token(now)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d parts", len(parts))
	}
	header, _ := base64.RawURLEncoding.DecodeString(parts[0])
	if string(header) != `{"alg":"HS256","typ":"JWT"}` {
		t.Errorf("header = %s", header)
	}
	payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatal(err)
	}
	if claims["sun"] != "slurmkube" || claims["iat"] != float64(now.Unix()) || claims["exp"] != float64(now.Add(10*time.Minute).Unix()) {
		t.Errorf("claims = %v", claims)
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if base64.RawURLEncoding.EncodeToString(mac.Sum(nil)) != parts[2] {
		t.Error("signature does not verify")
	}

	// Cached for half the lifetime, then renewed.
	again, _ := m.Token(now.Add(4 * time.Minute))
	if again != tok {
		t.Error("the token must be reused while fresh")
	}
	renewed, _ := m.Token(now.Add(6 * time.Minute))
	if renewed == tok {
		t.Error("the token must be renewed after half its lifetime")
	}
	if _, err := (&tokenMinter{keyFile: filepath.Join(t.TempDir(), "missing"), lifetime: time.Minute}).Token(now); err == nil {
		t.Error("a missing key file is an error")
	}
}
