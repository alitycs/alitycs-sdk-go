package alitycs

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
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
// requires.
func serializeProps(props Props) map[string]string {
	out := make(map[string]string, len(props))
	for key, value := range props {
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
	}
	return out
}

func stringify(value any) string {
	if encoded, err := json.Marshal(value); err == nil {
		return string(encoded)
	}
	return fmt.Sprintf("%v", value)
}

// nowMillis is a variable so tests can pin timestamps if needed.
var nowMillis = func() int64 { return time.Now().UnixMilli() }
