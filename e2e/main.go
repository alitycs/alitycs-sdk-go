// Command main emits an identify and a uniquely named track event through the
// real worker ingest endpoint, for the Go SDK e2e journey in
// alitycs-autotests/tests/e2e/sdk/go.test.ts. The test polls the analytics
// read API for these events afterwards — this process only proves the SDK
// sent them.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/alitycs/alitycs-sdk-go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "alitycs-e2e: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	apiKey := requiredEnvironment("ALITYCS_API_KEY")
	endpoint := requiredEnvironment("ALITYCS_ENDPOINT")
	runID := requiredEnvironment("ALITYCS_RUN_ID")
	phase := os.Getenv("ALITYCS_E2E_PHASE")
	stateFile := os.Getenv("ALITYCS_STATE_FILE")
	if phase == "first" {
		endpoint = requiredEnvironment("ALITYCS_FAILURE_ENDPOINT")
	}

	eventName := "sdk_go_track_" + runID
	userID := "sdk-go-user-" + runID

	options := []alitycs.Option{
		alitycs.WithEndpoint(endpoint),
		alitycs.WithFlushSize(10),
		alitycs.WithFlushInterval(0),
	}
	if stateFile != "" {
		options = append(options, alitycs.WithPersistence(stateFile), alitycs.WithMaxRetries(0))
	}
	client, err := alitycs.New(apiKey, options...)
	if err != nil {
		return err
	}
	ctx := context.Background()
	if phase == "first" {
		client.SetGlobalProperties(alitycs.Props{
			"test_run_id": runID,
			"sdk_package": "go",
			"scenario":    "go-restart",
		})
		client.Track(ctx, "sdk_go_restart_"+runID, nil)
		_ = client.Flush(ctx)
		os.Exit(0)
	}
	if phase == "restart" {
		if err := client.Flush(ctx); err != nil {
			return fmt.Errorf("restart flush: %w", err)
		}
		return client.Shutdown(context.Background())
	}

	client.SetGlobalProperties(alitycs.Props{
		"test_run_id": runID,
		"sdk_package": "go",
		"scenario":    "go-subprocess",
	})
	client.Identify(ctx, userID, alitycs.Props{"runtime": "go"})
	client.Track(ctx, eventName, alitycs.Props{"source": "go-sdk-e2e"})
	client.Track(ctx, "sdk_go_request_a_"+runID, nil, alitycs.WithUserID("sdk-go-request-a-"+runID))
	client.Track(ctx, "sdk_go_request_b_"+runID, nil, alitycs.WithUserID("sdk-go-request-b-"+runID))

	if err := client.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	if err := client.Shutdown(context.Background()); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}

	fmt.Printf("Go SDK e2e emitted identify and %s\n", eventName)
	return nil
}

func requiredEnvironment(name string) string {
	value := os.Getenv(name)
	if value == "" {
		fmt.Fprintf(os.Stderr, "alitycs-e2e: %s is required\n", name)
		os.Exit(1)
	}
	return value
}
