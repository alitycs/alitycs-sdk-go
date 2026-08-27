package alitycs

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Canonical ingestion limits, mirroring the server's EventValidator exactly.
// The server rejects an entire batch when a single event violates any of
// them, so violating events are rejected locally at build time: they are
// never queued and never sent (surfaced via warn log + Stats.Rejected).
const (
	maxPropertiesCount     = 50
	maxPropertyKeyLength   = 100
	maxPropertyValueLength = 1000
	maxEventSizeBytes      = 64 * 1024

	// eventSizeOverhead is the constant the server adds to every event when
	// estimating its wire size.
	eventSizeOverhead = 200

	// minEpochMillis is the smallest plausible epoch-milliseconds timestamp;
	// anything below it looks like seconds-scale.
	minEpochMillis = 1_000_000_000_000

	maxEventAgeDays = 7
)

// Props carries event properties. Values are serialized to strings for the
// wire contract: strings pass through, numbers and booleans use their
// canonical form, maps and slices are JSON-encoded, nil is skipped.
type Props map[string]any

// generateID returns 16 random bytes as hex. crypto/rand does not fail on
// supported platforms, but the fallback keeps identifiers non-empty even if
// that guarantee ever breaks.
func generateID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%032x", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}

// serializeProps converts event properties to the all-string shape the schema
// requires. It rejects property sets that violate the canonical ingestion
// limits instead of truncating or passing them through: the server would
// refuse the entire batch over a single offending event.
func serializeProps(props Props) (map[string]string, error) {
	if len(props) > maxPropertiesCount {
		return nil, fmt.Errorf("alitycs: event rejected locally: %d properties exceeds the maximum of %d per event", len(props), maxPropertiesCount)
	}
	out := make(map[string]string, len(props))
	for key, value := range props {
		if len(key) > maxPropertyKeyLength {
			return nil, fmt.Errorf("alitycs: event rejected locally: property key %q exceeds the maximum of %d characters", key, maxPropertyKeyLength)
		}
		if value == nil {
			continue
		}
		switch v := value.(type) {
		case string:
			out[key] = v
		case bool:
			out[key] = strconv.FormatBool(v)
		case int:
			out[key] = strconv.Itoa(v)
		case int64:
			out[key] = strconv.FormatInt(v, 10)
		case float64:
			out[key] = strconv.FormatFloat(v, 'f', -1, 64)
		default:
			out[key] = stringify(value)
		}
		if len(out[key]) > maxPropertyValueLength {
			return nil, fmt.Errorf("alitycs: event rejected locally: value for property key %q exceeds the maximum of %d characters", key, maxPropertyValueLength)
		}
	}
	return out, nil
}

func stringify(value any) string {
	if encoded, err := json.Marshal(value); err == nil {
		return string(encoded)
	}
	return fmt.Sprintf("%v", value)
}

// validateEvent checks a fully built event against the canonical server
// limits that are not already enforced by serializeProps: required fields,
// timestamp unit and age bounds, and the estimated wire size. Revenue payloads
// validate themselves via Revenue.Validate.
func validateEvent(event Event) error {
	var problems []string
	if event.Event == "" {
		problems = append(problems, "action is required and cannot be blank")
	}
	if event.UserID == "" && event.AnonymousID == "" {
		problems = append(problems, "at least one of userId or anonymousId is required")
	}
	if event.Timestamp < minEpochMillis {
		problems = append(problems, fmt.Sprintf("timestamp must be epoch milliseconds (got %d, which looks like seconds-scale)", event.Timestamp))
	} else {
		now := nowMillis()
		if event.Timestamp > now {
			problems = append(problems, "timestamp cannot be in the future")
		} else if event.Timestamp < now-maxEventAgeDays*24*60*60*1000 {
			problems = append(problems, fmt.Sprintf("timestamp is too old (older than %d days)", maxEventAgeDays))
		}
	}
	for key, value := range event.Properties {
		if len(key) > maxPropertyKeyLength {
			problems = append(problems, fmt.Sprintf("property key %q exceeds the maximum of %d characters", key, maxPropertyKeyLength))
		}
		if len(value) > maxPropertyValueLength {
			problems = append(problems, fmt.Sprintf("value for property key %q exceeds the maximum of %d characters", key, maxPropertyValueLength))
		}
	}
	estimated := len(event.UserID) + len(event.AnonymousID) + len(event.Event) + eventSizeOverhead
	for key, value := range event.Properties {
		estimated += len(key) + len(value)
	}
	if estimated > maxEventSizeBytes {
		problems = append(problems, fmt.Sprintf("event size (~%d bytes) exceeds the maximum allowed size (%d bytes)", estimated, maxEventSizeBytes))
	}
	if len(problems) > 0 {
		return fmt.Errorf("alitycs: event rejected locally: %v", problems)
	}
	return nil
}

// nowMillis is a variable so tests can pin timestamps if needed.
var nowMillis = func() int64 { return time.Now().UnixMilli() }
