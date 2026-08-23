package alitycs

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// eventType is the wire category of an event. Values must match
// alitycs-sdk-js/specs/event-schema.json v0.4.0 exactly.
type eventType string

const (
	eventTypeTrack    eventType = "track"
	eventTypeIdentify eventType = "identify"
	eventTypePage     eventType = "page"
	eventTypeError    eventType = "error"
)

const (
	prefixEvent   = "evt_"
	prefixBatch   = "batch_"
	prefixSession = "sess_"
	prefixAnon    = "anon_"
)

// Event is a single analytics event on the wire.
type Event struct {
	EventID     string            `json:"eventId"`
	Event       string            `json:"event"`
	EventType   eventType         `json:"eventType"`
	UserID      string            `json:"userId,omitempty"`
	AnonymousID string            `json:"anonymousId"`
	SessionID   string            `json:"sessionId"`
	Timestamp   int64             `json:"timestamp"`
	Properties  map[string]string `json:"properties"`
	Revenue     *Revenue          `json:"revenue,omitempty"`
	Context     Context           `json:"context"`
}

// BatchPayload wraps events for one ingest request.
type BatchPayload struct {
	BatchID string  `json:"batchId"`
	SentAt  int64   `json:"sentAt"`
	Events  []Event `json:"events"`
}

// Context describes the environment an event was emitted from. sdkVersion and
// sdkLanguage are required by the schema; every other field is optional.
type Context struct {
	SDKVersion  string `json:"sdkVersion"`
	SDKLanguage string `json:"sdkLanguage"`
	Locale      string `json:"locale,omitempty"`
	Timezone    string `json:"timezone,omitempty"`
	OSName      string `json:"osName,omitempty"`
	OSArch      string `json:"osArch,omitempty"`
	GoVersion   string `json:"goVersion,omitempty"`
}

// Revenue is the server-side trusted revenue payload attached to a track
// event by TrackRevenue. Build one with NewTransaction, NewMRRSnapshot or
// NewMRRBaselineComplete, or construct it directly and call Validate.
type Revenue struct {
	Version                     int    `json:"version"`
	Kind                        string `json:"kind"`
	FactID                      string `json:"factId"`
	Amount                      string `json:"amount,omitempty"`
	Currency                    string `json:"currency"`
	CustomerID                  string `json:"customerId,omitempty"`
	SubscriptionID              string `json:"subscriptionId,omitempty"`
	MRRAmount                   string `json:"mrrAmount,omitempty"`
	ExpectedActiveSubscriptions *int   `json:"expectedActiveSubscriptions,omitempty"`
}

// Revenue kinds — one schema variant each.
const (
	KindTransaction         = "transaction"
	KindMRRSnapshot         = "mrr_snapshot"
	KindMRRBaselineComplete = "mrr_baseline_complete"
)

var (
	revenueCurrencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
	revenueAmountPattern   = regexp.MustCompile(`^-?(?:0|[1-9]\d*)(?:\.\d{1,9})?$`)
)

// NewTransaction returns a validated transaction revenue payload.
func NewTransaction(factID, amount, currency string) (Revenue, error) {
	return validateRevenue(Revenue{
		Version:  1,
		Kind:     KindTransaction,
		FactID:   factID,
		Amount:   amount,
		Currency: currency,
	})
}

// NewMRRSnapshot returns a validated monthly recurring revenue snapshot.
func NewMRRSnapshot(factID, subscriptionID, customerID, mrrAmount, currency string) (Revenue, error) {
	return validateRevenue(Revenue{
		Version:        1,
		Kind:           KindMRRSnapshot,
		FactID:         factID,
		SubscriptionID: subscriptionID,
		CustomerID:     customerID,
		MRRAmount:      mrrAmount,
		Currency:       currency,
	})
}

// NewMRRBaselineComplete returns a validated MRR baseline completion payload.
func NewMRRBaselineComplete(factID, currency string, expectedActiveSubscriptions int) (Revenue, error) {
	return validateRevenue(Revenue{
		Version:                     1,
		Kind:                        KindMRRBaselineComplete,
		FactID:                      factID,
		Currency:                    currency,
		ExpectedActiveSubscriptions: &expectedActiveSubscriptions,
	})
}

// Validate checks a Revenue payload against the schema variant its Kind
// selects.
func (r Revenue) Validate() error {
	_, err := validateRevenue(r)
	return err
}

func validateRevenue(r Revenue) (Revenue, error) {
	if r.Version != 1 {
		return Revenue{}, errors.New("alitycs: revenue version must be 1")
	}
	if len(r.FactID) == 0 || len(r.FactID) > 200 {
		return Revenue{}, errors.New("alitycs: revenue factId must be between 1 and 200 characters")
	}
	if !revenueCurrencyPattern.MatchString(r.Currency) {
		return Revenue{}, errors.New("alitycs: revenue currency must be a three-letter uppercase code")
	}

	switch r.Kind {
	case KindTransaction:
		if err := validateDecimal("amount", r.Amount); err != nil {
			return Revenue{}, err
		}
	case KindMRRSnapshot:
		if strings.TrimSpace(r.SubscriptionID) == "" {
			return Revenue{}, errors.New("alitycs: mrr_snapshot requires subscriptionId")
		}
		if strings.TrimSpace(r.CustomerID) == "" {
			return Revenue{}, errors.New("alitycs: mrr_snapshot requires customerId")
		}
		if err := validateDecimal("mrrAmount", r.MRRAmount); err != nil {
			return Revenue{}, err
		}
		if strings.HasPrefix(r.MRRAmount, "-") {
			return Revenue{}, errors.New("alitycs: mrr_snapshot amount must be non-negative")
		}
	case KindMRRBaselineComplete:
		if r.ExpectedActiveSubscriptions == nil {
			return Revenue{}, errors.New("alitycs: mrr_baseline_complete requires expectedActiveSubscriptions")
		}
		if *r.ExpectedActiveSubscriptions < 0 {
			return Revenue{}, errors.New("alitycs: expectedActiveSubscriptions must be non-negative")
		}
	default:
		return Revenue{}, fmt.Errorf("alitycs: unknown revenue kind %q", r.Kind)
	}

	return r, nil
}

func validateDecimal(field, value string) error {
	if !revenueAmountPattern.MatchString(value) {
		return fmt.Errorf("alitycs: revenue %s must be a non-exponent decimal string with at most 9 fraction digits", field)
	}
	digits := len(strings.ReplaceAll(strings.ReplaceAll(value, "-", ""), ".", ""))
	if digits > 38 {
		return fmt.Errorf("alitycs: revenue %s must not exceed 38 digits of precision", field)
	}
	return nil
}
