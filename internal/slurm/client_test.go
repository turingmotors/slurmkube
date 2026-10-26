// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fakeRestd is slurmrestd as far as the client cares: it records requests
// and answers from a table keyed by "METHOD path".
type fakeRestd struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   []string
	answers  map[string]answer
}

type answer struct {
	status int
	body   string
}

func (f *fakeRestd) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.requests = append(f.requests, r)
	f.bodies = append(f.bodies, string(body))
	a, ok := f.answers[r.Method+" "+r.URL.Path]
	f.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"errors":[{"error_number":2017,"error":"Invalid job id specified","description":"Job not found"}]}`))
		return
	}
	w.WriteHeader(a.status)
	_, _ = w.Write([]byte(a.body))
}

func newTestClient(t *testing.T, f *fakeRestd, mod func(*Options)) *Client {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("secret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	o := Options{BaseURL: srv.URL + "/", User: "slurmkube", TokenFile: tokenFile}
	if mod != nil {
		mod(&o)
	}
	c, err := NewClient(o)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestListJobsDecodesAndAuthenticates(t *testing.T) {
	f := &fakeRestd{answers: map[string]answer{
		"GET " + apiPrefix + "/jobs": {200, `{"jobs":[
			{"job_id":7,"name":"a","partition":"k8s","job_state":["PENDING"],"hold":true,
			 "tasks":{"set":true,"infinite":false,"number":2},"time_limit":{"set":true,"infinite":true,"number":0},
			 "tres_per_node":"gres/gpu:1","extra":"slurmkube.nodelist=gpu-1"},
			{"job_id":8,"job_state":["RUNNING"]}],"errors":[],"warnings":[]}`},
	}}
	var calls []string
	c := newTestClient(t, f, nil)
	c.OnRequest = func(method string, status int) { calls = append(calls, method+" "+http.StatusText(status)) }
	jobs, err := c.ListJobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 || jobs[0].JobID != 7 || jobs[0].NTasks() != 2 || jobs[0].TimeLimitSeconds() != 0 || jobs[0].GPUsPerNode() != 1 || !jobs[0].IsHeld() {
		t.Errorf("jobs = %+v", jobs)
	}
	req := f.requests[0]
	if req.Header.Get("X-SLURM-USER-NAME") != "slurmkube" || req.Header.Get("X-SLURM-USER-TOKEN") != "secret-token" {
		t.Errorf("auth headers = %v", req.Header)
	}
	if len(calls) != 1 || calls[0] != "GET OK" {
		t.Errorf("OnRequest calls = %v", calls)
	}
}

func TestJWTKeyMintsTokens(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "jwt.key")
	if err := os.WriteFile(keyFile, []byte("k"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := &fakeRestd{answers: map[string]answer{"GET " + apiPrefix + "/nodes": {200, `{"nodes":[{"name":"slurmkube-w0-7","port":63000,"address":"10.0.0.2","extra":"uid-1","state":["IDLE","DYNAMIC_NORM"],"features":["slurmkube-pin-7"]}]}`}}}
	c := newTestClient(t, f, func(o *Options) { o.TokenFile = ""; o.User = ""; o.JWTKeyFile = keyFile; o.JWTUser = "restd" })
	nodes, err := c.ListNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].Port != 63000 || !nodes[0].HasState("dynamic_norm") || !nodes[0].HasFeature("slurmkube-pin-7") {
		t.Errorf("nodes = %+v", nodes)
	}
	req := f.requests[0]
	if req.Header.Get("X-SLURM-USER-NAME") != "restd" || strings.Count(req.Header.Get("X-SLURM-USER-TOKEN"), ".") != 2 {
		t.Errorf("auth headers = %v", req.Header)
	}
}

func TestErrorsAreClassified(t *testing.T) {
	f := &fakeRestd{answers: map[string]answer{
		"DELETE " + apiPrefix + "/node/busy":        {422, `{"errors":[{"error_number":2016,"error":"Requested nodes are busy","description":"a job is running","source":"node"}]}`},
		"DELETE " + apiPrefix + "/node/none":        {422, `{"errors":[{"error_number":2018,"error":"Invalid node name specified"}]}`},
		"POST " + apiPrefix + "/job/1":              {200, `{"errors":[{"error_number":9999,"error":"Invalid feature specification"}]}`},
		"POST " + apiPrefix + "/job/2":              {500, `{"errors":[{"error_number":9998,"error":"Requested partition configuration not available now"}]}`},
		"POST " + apiPrefix + "/job/3":              {502, `upstream gone`},
		"GET " + apiPrefix + "/job/4":               {200, `{"jobs":[]}`},
		"DELETE " + apiPrefix + "/job/5":            {200, `{}`},
		"GET " + apiPrefix + "/node/slurmkube-w0-1": {200, `{"nodes":[{"name":"slurmkube-w0-1"}]}`},
	}}
	c := newTestClient(t, f, nil)
	ctx := context.Background()

	err := c.DeleteNode(ctx, "busy")
	if !IsNodeBusy(err) || IsNotFound(err) {
		t.Errorf("busy: %v", err)
	}
	var e *Error
	if !errors.As(err, &e) || e.Status != 422 || len(e.Entries) != 1 || e.Entries[0].Number != 2016 || !strings.Contains(err.Error(), "DELETE") {
		t.Errorf("Error = %+v (%v)", e, err)
	}
	if err := c.DeleteNode(ctx, "none"); !IsNotFound(err) || IsNodeBusy(err) {
		t.Errorf("none: %v", err)
	}
	if err := c.SetJobFeatures(ctx, 1, "slurmkube-pin-1"); !IsFeatureUnknown(err) {
		t.Errorf("a 200 with errors is still an error: %v", err)
	}
	if err := c.HoldJob(ctx, 2); !IsPinUnavailable(err) || IsFeatureUnknown(err) {
		t.Errorf("pin unavailable: %v", err)
	}
	err = c.ReleaseJob(ctx, 3)
	if !errors.As(err, &e) || e.Status != 502 || !strings.Contains(err.Error(), "upstream gone") {
		t.Errorf("non-JSON failure: %v", err)
	}
	if IsNotFound(err) || IsNodeBusy(err) || IsFeatureUnknown(err) || IsPinUnavailable(err) || IsNotFound(nil) || IsNodeBusy(errors.New("x")) {
		t.Error("classification must be specific")
	}
	if _, found, err := c.GetJob(ctx, 4); err != nil || found {
		t.Errorf("empty list = not found: %v %v", found, err)
	}
	if _, found, err := c.GetJob(ctx, 99); err != nil || found {
		t.Errorf("404 = not found, no error: %v %v", found, err)
	}
	if err := c.CancelJob(ctx, 99); err != nil {
		t.Errorf("cancelling a gone job is fine: %v", err)
	}
	if err := c.CancelJob(ctx, 5); err != nil {
		t.Errorf("cancel: %v", err)
	}
	if n, found, err := c.GetNode(ctx, "slurmkube-w0-1"); err != nil || !found || n.Name != "slurmkube-w0-1" {
		t.Errorf("GetNode = %+v %v %v", n, found, err)
	}
	if _, found, err := c.GetNode(ctx, "other"); err != nil || found {
		t.Errorf("unknown node: %v %v", found, err)
	}
}

func TestUpdatesSendWhatSlurmctldExpects(t *testing.T) {
	f := &fakeRestd{answers: map[string]answer{
		"POST " + apiPrefix + "/job/1":     {200, `{}`},
		"POST " + apiPrefix + "/node/w0-1": {200, `{}`},
	}}
	c := newTestClient(t, f, nil)
	ctx := context.Background()
	for _, call := range []func() error{
		func() error { return c.HoldJob(ctx, 1) },
		func() error { return c.ReleaseJob(ctx, 1) },
		func() error { return c.SetJobFeatures(ctx, 1, "slurmkube-pin-1") },
		func() error { return c.SetJobComment(ctx, 1, "slurmkube: held:\nline two\x01") },
		func() error { return c.SetNodeState(ctx, "w0-1", "DOWN", "slurmkube: pod gone") },
	} {
		if err := call(); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{
		`{"hold":true}`, `{"hold":false}`, `{"constraints":"slurmkube-pin-1"}`,
		`{"comment":"slurmkube: held: line two"}`, `{"reason":"slurmkube: pod gone","state":["DOWN"]}`,
	}
	for i, w := range want {
		var got, exp any
		_ = json.Unmarshal([]byte(f.bodies[i]), &got)
		_ = json.Unmarshal([]byte(w), &exp)
		if g, _ := json.Marshal(got); string(g) != w {
			t.Errorf("request %d body = %s, want %s", i, f.bodies[i], w)
		}
		if f.requests[i].Header.Get("Content-Type") != "application/json" {
			t.Errorf("request %d has no JSON content type", i)
		}
	}
}

func TestNewClientValidatesFiles(t *testing.T) {
	if _, err := NewClient(Options{BaseURL: "https://x", TokenFile: "/nonexistent/token"}); err == nil {
		t.Error("a missing token file is an error")
	}
	if _, err := NewClient(Options{BaseURL: "https://x", CACertFile: "/nonexistent/ca.crt"}); err == nil {
		t.Error("a missing CA bundle is an error")
	}
	bad := filepath.Join(t.TempDir(), "ca.crt")
	_ = os.WriteFile(bad, []byte("not a cert"), 0o600)
	if _, err := NewClient(Options{BaseURL: "https://x", CACertFile: bad}); err == nil {
		t.Error("a CA bundle without certificates is an error")
	}
}
