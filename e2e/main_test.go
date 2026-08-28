package main

import "testing"

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
