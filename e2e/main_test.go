package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateRestartState(t *testing.T) {
	for _, phase := range []string{"first", "restart"} {
		if err := validateRestartState(phase, ""); err == nil {
			t.Fatalf("%s phase accepted an empty state file", phase)
		}
		if err := validateRestartState(phase, "/tmp/alitycs-e2e-wal.json"); err != nil {
			t.Fatalf("%s phase rejected a state file: %v", phase, err)
		}
	}
	if err := validateRestartState("", ""); err != nil {
		t.Fatalf("ordinary phase unexpectedly requires persistence: %v", err)
	}
}

func TestRunPersistsFailedPhaseAndReplaysOnRestart(t *testing.T) {
	failureBodies := make(chan string, 4)
	failureServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		failureBodies <- string(body)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failureServer.Close()

	successBodies := make(chan string, 1)
	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		successBodies <- string(body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer successServer.Close()

	stateFile := filepath.Join(t.TempDir(), "go-e2e-wal.json")
	t.Setenv("ALITYCS_API_KEY", "pk_e2e_test")
	t.Setenv("ALITYCS_ENDPOINT", successServer.URL)
	t.Setenv("ALITYCS_FAILURE_ENDPOINT", failureServer.URL)
	t.Setenv("ALITYCS_RUN_ID", "durable-flow")
	t.Setenv("ALITYCS_STATE_FILE", stateFile)
	t.Setenv("ALITYCS_E2E_PHASE", "first")
	if err := run(); err != nil {
		t.Fatalf("first phase: %v", err)
	}
	var firstBody string
	select {
	case body := <-failureBodies:
		firstBody = body
		if !strings.Contains(body, `"event":"sdk_go_restart_durable-flow"`) {
			t.Fatalf("first-phase body = %s", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first phase did not call the failure endpoint")
	}
	raw, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("read first-phase WAL: %v", err)
	}
	if !strings.Contains(string(raw), `sdk_go_restart_durable-flow`) {
		t.Fatalf("first-phase WAL does not retain the restart event: %s", raw)
	}

	t.Setenv("ALITYCS_E2E_PHASE", "restart")
	if err := run(); err != nil {
		t.Fatalf("restart phase: %v", err)
	}
	select {
	case body := <-successBodies:
		if body != firstBody {
			t.Fatalf("restart changed the retained request body:\nfirst:   %s\nrestart: %s", firstBody, body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("restart phase did not replay the retained event")
	}
	if _, err := os.Stat(stateFile); !os.IsNotExist(err) {
		t.Fatalf("restart left WAL behind: %v", err)
	}
}
