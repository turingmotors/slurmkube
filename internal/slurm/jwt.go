// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// tokenMinter signs short-lived HS256 tokens with slurmctld's JWT key
// (the same key slurmrestd verifies against), the way `scontrol token`
// does, so no long-lived token has to be stored.
type tokenMinter struct {
	keyFile  string
	user     string
	lifetime time.Duration

	mu      sync.Mutex
	token   string
	renewAt time.Time
}

func (m *tokenMinter) Token(now time.Time) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.token != "" && now.Before(m.renewAt) {
		return m.token, nil
	}
	key, err := os.ReadFile(m.keyFile)
	if err != nil {
		return "", fmt.Errorf("reading jwt key: %w", err)
	}
	claims := map[string]any{"iat": now.Unix(), "exp": now.Add(m.lifetime).Unix(), "sun": m.user}
	tok, err := signHS256(key, claims)
	if err != nil {
		return "", err
	}
	m.token = tok
	m.renewAt = now.Add(m.lifetime / 2)
	return tok, nil
}

func signHS256(key []byte, claims map[string]any) (string, error) {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signing := header + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(signing))
	return signing + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}
