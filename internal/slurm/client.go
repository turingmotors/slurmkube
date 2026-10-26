// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const apiPrefix = "/slurm/v0.0.44"

// Options configures a Client.
type Options struct {
	BaseURL string
	// User is sent as X-SLURM-USER-NAME.
	User string
	// TokenFile holds a token for X-SLURM-USER-TOKEN; or JWTKeyFile mints
	// one per JWTLifetime for JWTUser.
	TokenFile          string
	JWTKeyFile         string
	JWTUser            string
	JWTLifetime        time.Duration
	CACertFile         string
	InsecureSkipVerify bool
	RequestTimeout     time.Duration
	RequestsPerSecond  float64
}

// Client is a slurmrestd client.
type Client struct {
	baseURL string
	user    string
	token   string
	minter  *tokenMinter
	limiter *rate.Limiter
	http    *http.Client
	// OnRequest and OnRequestDuration feed metrics.
	OnRequest         func(method string, status int)
	OnRequestDuration func(method string, d time.Duration)
}

func NewClient(o Options) (*Client, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: o.InsecureSkipVerify} //nolint:gosec // opt-in
	if o.CACertFile != "" {
		pem, err := os.ReadFile(o.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA bundle: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA bundle %q holds no certificate", o.CACertFile)
		}
		tlsCfg.RootCAs = pool
	}
	c := &Client{
		baseURL: strings.TrimRight(o.BaseURL, "/"),
		user:    o.User,
		http:    &http.Client{Timeout: cmp(o.RequestTimeout, 30*time.Second), Transport: &http.Transport{TLSClientConfig: tlsCfg}},
	}
	if o.JWTKeyFile != "" {
		c.minter = &tokenMinter{keyFile: o.JWTKeyFile, user: o.JWTUser, lifetime: cmp(o.JWTLifetime, 10*time.Minute)}
		if c.user == "" {
			c.user = o.JWTUser
		}
	} else if o.TokenFile != "" {
		raw, err := os.ReadFile(o.TokenFile)
		if err != nil {
			return nil, fmt.Errorf("reading token: %w", err)
		}
		c.token = strings.TrimSpace(string(raw))
	}
	if o.RequestsPerSecond > 0 {
		c.limiter = rate.NewLimiter(rate.Limit(o.RequestsPerSecond), max(int(o.RequestsPerSecond), 1))
	}
	return c, nil
}

func cmp(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	return v
}

// Error is a slurmrestd failure: the HTTP status and the Slurm error
// entries of the body.
type Error struct {
	Status  int
	Method  string
	Path    string
	Entries []ErrorEntry
	Body    string
}

// ErrorEntry is one element of the body's "errors" array.
type ErrorEntry struct {
	Number      int    `json:"error_number"`
	Message     string `json:"error"`
	Description string `json:"description"`
	Source      string `json:"source"`
}

func (e *Error) Error() string {
	msgs := make([]string, 0, len(e.Entries))
	for _, en := range e.Entries {
		msgs = append(msgs, fmt.Sprintf("%d %s (%s)", en.Number, en.Message, en.Description))
	}
	if len(msgs) == 0 {
		return fmt.Sprintf("slurmrestd %s %s: HTTP %d: %s", e.Method, e.Path, e.Status, truncate(e.Body, 300))
	}
	return fmt.Sprintf("slurmrestd %s %s: HTTP %d: %s", e.Method, e.Path, e.Status, strings.Join(msgs, "; "))
}

func (e *Error) has(number int, text string) bool {
	for _, en := range e.Entries {
		if number != 0 && en.Number == number {
			return true
		}
		if text != "" && (strings.Contains(en.Message, text) || strings.Contains(en.Description, text)) {
			return true
		}
	}
	return text != "" && strings.Contains(e.Body, text)
}

// Slurm error numbers slurmctld answers with.
const (
	errNodesBusy       = 2016 // "Requested nodes are busy": a job is on the node
	errInvalidNodeName = 2018 // "Invalid node name specified"
)

// IsNotFound: the job (404) or node (422/2018) does not exist.
func IsNotFound(err error) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	return e.Status == http.StatusNotFound || e.has(errInvalidNodeName, "Invalid node name")
}

// IsNodeBusy: a node cannot be deleted while a job is allocated to or
// completing on it.
func IsNodeBusy(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.has(errNodesBusy, "nodes are busy")
}

// IsFeatureUnknown: a job update named a feature no node advertises yet.
func IsFeatureUnknown(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.has(0, "Invalid feature specification")
}

// IsPinUnavailable: slurmctld refuses a job update while no node satisfies
// the job's constraints (its records are being replaced).
func IsPinUnavailable(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.has(0, "Requested partition configuration not available now")
}

// Jobs ----------------------------------------------------------------

// ListJobs returns every job slurmctld knows (running and recently ended).
func (c *Client) ListJobs(ctx context.Context) ([]Job, error) {
	var out struct {
		Jobs []Job `json:"jobs"`
	}
	if err := c.do(ctx, http.MethodGet, apiPrefix+"/jobs", nil, &out); err != nil {
		return nil, err
	}
	return out.Jobs, nil
}

func jobPath(id uint64) string { return apiPrefix + "/job/" + strconv.FormatUint(id, 10) }

// GetJob reads one job; found is false when slurmctld no longer has it.
func (c *Client) GetJob(ctx context.Context, id uint64) (*Job, bool, error) {
	var out struct {
		Jobs []Job `json:"jobs"`
	}
	switch err := c.do(ctx, http.MethodGet, jobPath(id), nil, &out); {
	case IsNotFound(err):
	case err != nil:
		return nil, false, err
	case len(out.Jobs) > 0:
		return &out.Jobs[0], true, nil
	}
	return nil, false, nil
}

// updateJob is `scontrol update JobId=<id> ...`.
func (c *Client) updateJob(ctx context.Context, id uint64, body map[string]any) error {
	return c.do(ctx, http.MethodPost, jobPath(id), body, nil)
}

func (c *Client) HoldJob(ctx context.Context, id uint64) error {
	return c.updateJob(ctx, id, map[string]any{"hold": true})
}

func (c *Client) ReleaseJob(ctx context.Context, id uint64) error {
	return c.updateJob(ctx, id, map[string]any{"hold": false})
}

// SetJobFeatures replaces the job's constraint (the pin to its nodes). The
// job record reports it as "features"; the update message names it
// "constraints" (v0.0.44 job_desc_msg) — "features" is accepted and
// silently ignored.
func (c *Client) SetJobFeatures(ctx context.Context, id uint64, features string) error {
	return c.updateJob(ctx, id, map[string]any{"constraints": features})
}

func (c *Client) SetJobComment(ctx context.Context, id uint64, comment string) error {
	return c.updateJob(ctx, id, map[string]any{"comment": sanitize(comment)})
}

// CancelJob is scancel.
func (c *Client) CancelJob(ctx context.Context, id uint64) error {
	if err := c.do(ctx, http.MethodDelete, jobPath(id), nil, nil); !IsNotFound(err) {
		return err
	}
	return nil
}

// Nodes ---------------------------------------------------------------

func (c *Client) ListNodes(ctx context.Context) ([]Node, error) {
	var out struct {
		Nodes []Node `json:"nodes"`
	}
	if err := c.do(ctx, http.MethodGet, apiPrefix+"/nodes", nil, &out); err != nil {
		return nil, err
	}
	return out.Nodes, nil
}

func (c *Client) GetNode(ctx context.Context, name string) (*Node, bool, error) {
	var out struct {
		Nodes []Node `json:"nodes"`
	}
	err := c.do(ctx, http.MethodGet, apiPrefix+"/node/"+name, nil, &out)
	if IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if len(out.Nodes) == 0 {
		return nil, false, nil
	}
	return &out.Nodes[0], true, nil
}

// DeleteNode removes a dynamic node record (refused while a job is on it:
// IsNodeBusy).
func (c *Client) DeleteNode(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, apiPrefix+"/node/"+name, nil, nil)
}

// SetNodeState is `scontrol update NodeName=<name> State=<state> Reason=`.
func (c *Client) SetNodeState(ctx context.Context, name, state, reason string) error {
	return c.do(ctx, http.MethodPost, apiPrefix+"/node/"+name, map[string]any{"state": []string{state}, "reason": sanitize(reason)}, nil)
}

// Transport -------------------------------------------------------------

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	if c.limiter != nil {
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
	}
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	token := c.token
	if c.minter != nil {
		if token, err = c.minter.Token(time.Now()); err != nil {
			return fmt.Errorf("minting slurm token: %w", err)
		}
	}
	if token != "" {
		req.Header.Set("X-SLURM-USER-TOKEN", token)
	}
	if c.user != "" {
		req.Header.Set("X-SLURM-USER-NAME", c.user)
	}
	start := time.Now()
	resp, err := c.http.Do(req)
	if c.OnRequestDuration != nil {
		c.OnRequestDuration(method, time.Since(start))
	}
	if err != nil {
		if c.OnRequest != nil {
			c.OnRequest(method, 0)
		}
		return fmt.Errorf("slurmrestd %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	if c.OnRequest != nil {
		c.OnRequest(method, resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return fmt.Errorf("slurmrestd %s %s: reading response: %w", method, path, err)
	}
	// slurmrestd reports failures in an "errors" array, with a 4xx/5xx or
	// (for some updates) a 200.
	var envelope struct {
		Errors []ErrorEntry `json:"errors"`
	}
	_ = json.Unmarshal(raw, &envelope)
	if resp.StatusCode >= 300 || len(envelope.Errors) > 0 {
		return &Error{Status: resp.StatusCode, Method: method, Path: path, Entries: envelope.Errors, Body: string(raw)}
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("slurmrestd %s %s: decoding response: %w", method, path, err)
		}
	}
	return nil
}

// sanitize keeps a comment or reason to one printable line slurmctld
// accepts.
func sanitize(s string) string {
	s = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return ' '
		}
		if r < 0x20 {
			return -1
		}
		return r
	}, s)
	if len(s) > 1000 {
		s = s[:1000]
	}
	return s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
