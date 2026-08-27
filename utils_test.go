package alitycs

import (
	"strings"
	"testing"
)

func TestSerializePropsRejectsOversizedKey(t *testing.T) {
	_, err := serializeProps(Props{strings.Repeat("k", 101): "v"})
	if err == nil || !strings.Contains(err.Error(), "property key") {
		t.Fatalf("serializeProps err = %v, want an oversized-key rejection", err)
	}
}

func TestSerializePropsRejectsOversizedValue(t *testing.T) {
	_, err := serializeProps(Props{"key": strings.Repeat("v", maxPropertyValueLength+1)})
	if err == nil || !strings.Contains(err.Error(), "value for property key") {
		t.Fatalf("serializeProps err = %v, want an oversized-value rejection", err)
	}
}

func TestSerializePropsRejectsTooManyProperties(t *testing.T) {
	props := make(Props, maxPropertiesCount+1)
	for i := 0; i < maxPropertiesCount+1; i++ {
		props["key"+string(rune('a'+i%26))+string(rune('a'+i/26))] = "v"
	}
	_, err := serializeProps(props)
	if err == nil || !strings.Contains(err.Error(), "exceeds the maximum") {
		t.Fatalf("serializeProps err = %v, want a count rejection", err)
	}
}

func TestSerializePropsAcceptsValuesExactlyAtTheLimits(t *testing.T) {
	got, err := serializeProps(Props{
		strings.Repeat("k", maxPropertyKeyLength): strings.Repeat("v", maxPropertyValueLength),
		"second": nil,
	})
	if err != nil {
		t.Fatalf("boundary values rejected: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d properties, want 1 (nil skipped)", len(got))
	}
}

func TestValidateEventAcceptsAWellFormedEvent(t *testing.T) {
	event := testEvent("fine")
	event.Timestamp = nowMillis()
	event.Properties = map[string]string{"key": "value"}
	if err := validateEvent(event); err != nil {
		t.Fatalf("validateEvent rejected a well-formed event: %v", err)
	}
}

func TestValidateEventRejectsSecondsScaleTimestamp(t *testing.T) {
	event := testEvent("seconds")
	event.Timestamp = nowMillis() / 1000
	err := validateEvent(event)
	if err == nil || !strings.Contains(err.Error(), "epoch milliseconds") {
		t.Fatalf("validateEvent err = %v, want seconds-scale rejection", err)
	}
}

func TestValidateEventRejectsFutureAndAncientTimestamps(t *testing.T) {
	now := nowMillis()

	future := testEvent("future")
	future.Timestamp = now + 60_000
	if err := validateEvent(future); err == nil || !strings.Contains(err.Error(), "future") {
		t.Fatalf("validateEvent err = %v, want future-timestamp rejection", err)
	}

	ancient := testEvent("ancient")
	ancient.Timestamp = now - (maxEventAgeDays+1)*24*60*60*1000
	if err := validateEvent(ancient); err == nil || !strings.Contains(err.Error(), "too old") {
		t.Fatalf("validateEvent err = %v, want age rejection", err)
	}
}

func TestValidateEventRejectsEventsOverTheEstimatedSizeLimit(t *testing.T) {
	event := testEvent("big")
	event.Properties = map[string]string{"big": strings.Repeat("v", maxEventSizeBytes)}
	err := validateEvent(event)
	if err == nil || !strings.Contains(err.Error(), "maximum allowed size") {
		t.Fatalf("validateEvent err = %v, want size-limit rejection", err)
	}
}

func TestValidateEventAccumulatesViolations(t *testing.T) {
	event := testEvent("") // blank action
	event.Timestamp = 42
	event.Properties = map[string]string{strings.Repeat("k", 101): strings.Repeat("v", 1001)}
	err := validateEvent(event)
	if err == nil {
		t.Fatal("validateEvent accepted an event with multiple violations")
	}
	for _, fragment := range []string{"action is required", "epoch milliseconds", "property key"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("error %q missing fragment %q", err, fragment)
		}
	}
}
