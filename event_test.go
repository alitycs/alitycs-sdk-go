package alitycs

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNewTransactionValidAndJSONShape(t *testing.T) {
	revenue, err := NewTransaction("fact-1", "19.99", "USD")
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}

	encoded, err := json.Marshal(revenue)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := map[string]bool{"version": true, "kind": true, "factId": true, "amount": true, "currency": true}
	for key := range decoded {
		if !want[key] {
			t.Errorf("transaction JSON carries unexpected key %q (schema forbids extras): %s", key, encoded)
		}
	}
	for key := range want {
		if _, ok := decoded[key]; !ok {
			t.Errorf("transaction JSON missing required key %q: %s", key, encoded)
		}
	}
}

func TestNewMRRSnapshotValidAndJSONShape(t *testing.T) {
	revenue, err := NewMRRSnapshot("fact-2", "sub-9", "cust-9", "250.00", "EUR")
	if err != nil {
		t.Fatalf("NewMRRSnapshot: %v", err)
	}
	encoded, err := json.Marshal(revenue)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{`"kind":"mrr_snapshot"`, `"subscriptionId":"sub-9"`, `"customerId":"cust-9"`, `"mrrAmount":"250.00"`} {
		if !strings.Contains(string(encoded), key) {
			t.Errorf("snapshot JSON missing %s: %s", key, encoded)
		}
	}
	if strings.Contains(string(encoded), `"amount":`) {
		t.Errorf("snapshot must not carry the transaction amount field: %s", encoded)
	}
}

func TestNewMRRBaselineCompleteZeroSubscriptionsSurvives(t *testing.T) {
	revenue, err := NewMRRBaselineComplete("fact-3", "USD", 0)
	if err != nil {
		t.Fatalf("NewMRRBaselineComplete(0): %v", err)
	}
	encoded, err := json.Marshal(revenue)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(encoded), `"expectedActiveSubscriptions":0`) {
		t.Fatalf("zero expectedActiveSubscriptions omitted: %s", encoded)
	}
}

func TestRevenueValidationFailures(t *testing.T) {
	cases := []struct {
		name    string
		revenue Revenue
	}{
		{"version", Revenue{Version: 2, Kind: KindTransaction, FactID: "f", Amount: "1.00", Currency: "USD"}},
		{"blank factId", Revenue{Version: 1, Kind: KindTransaction, FactID: "", Amount: "1.00", Currency: "USD"}},
		{"long factId", Revenue{Version: 1, Kind: KindTransaction, FactID: strings.Repeat("x", 201), Amount: "1.00", Currency: "USD"}},
		{"currency case", Revenue{Version: 1, Kind: KindTransaction, FactID: "f", Amount: "1.00", Currency: "usd"}},
		{"currency length", Revenue{Version: 1, Kind: KindTransaction, FactID: "f", Amount: "1.00", Currency: "USDT"}},
		{"unknown kind", Revenue{Version: 1, Kind: "payout", FactID: "f", Currency: "USD"}},
		{"bad amount", Revenue{Version: 1, Kind: KindTransaction, FactID: "f", Amount: "1e5", Currency: "USD"}},
		{"empty amount", Revenue{Version: 1, Kind: KindTransaction, FactID: "f", Amount: "", Currency: "USD"}},
		{"exponent amount", Revenue{Version: 1, Kind: KindTransaction, FactID: "f", Amount: "-0.0000000001e3", Currency: "USD"}},
		{"precision", Revenue{Version: 1, Kind: KindTransaction, FactID: "f", Amount: strings.Repeat("9", 39), Currency: "USD"}},
		{"missing subscription", Revenue{Version: 1, Kind: KindMRRSnapshot, FactID: "f", CustomerID: "c", MRRAmount: "5.00", Currency: "USD"}},
		{"missing customer", Revenue{Version: 1, Kind: KindMRRSnapshot, FactID: "f", SubscriptionID: "s", MRRAmount: "5.00", Currency: "USD"}},
		{"negative mrr", Revenue{Version: 1, Kind: KindMRRSnapshot, FactID: "f", SubscriptionID: "s", CustomerID: "c", MRRAmount: "-5.00", Currency: "USD"}},
		{"nil subscriptions", Revenue{Version: 1, Kind: KindMRRBaselineComplete, FactID: "f", Currency: "USD"}},
		{"negative subscriptions", func() Revenue {
			n := -1
			return Revenue{Version: 1, Kind: KindMRRBaselineComplete, FactID: "f", Currency: "USD", ExpectedActiveSubscriptions: &n}
		}()},
		// Per-kind exclusivity mirrors the server: foreign fields are rejected.
		{"transaction with subscriptionId", Revenue{Version: 1, Kind: KindTransaction, FactID: "f", Amount: "1.00", Currency: "USD", SubscriptionID: "s"}},
		{"transaction with mrrAmount", Revenue{Version: 1, Kind: KindTransaction, FactID: "f", Amount: "1.00", Currency: "USD", MRRAmount: "5.00"}},
		{"transaction with expectedActiveSubscriptions", func() Revenue {
			n := 3
			return Revenue{Version: 1, Kind: KindTransaction, FactID: "f", Amount: "1.00", Currency: "USD", ExpectedActiveSubscriptions: &n}
		}()},
		{"snapshot with amount", Revenue{Version: 1, Kind: KindMRRSnapshot, FactID: "f", SubscriptionID: "s", CustomerID: "c", MRRAmount: "5.00", Currency: "USD", Amount: "1.00"}},
		{"snapshot with expectedActiveSubscriptions", func() Revenue {
			n := 3
			return Revenue{Version: 1, Kind: KindMRRSnapshot, FactID: "f", SubscriptionID: "s", CustomerID: "c", MRRAmount: "5.00", Currency: "USD", ExpectedActiveSubscriptions: &n}
		}()},
		{"baseline with amount", func() Revenue {
			n := 3
			return Revenue{Version: 1, Kind: KindMRRBaselineComplete, FactID: "f", Currency: "USD", ExpectedActiveSubscriptions: &n, Amount: "1.00"}
		}()},
		{"baseline with mrrAmount", func() Revenue {
			n := 3
			return Revenue{Version: 1, Kind: KindMRRBaselineComplete, FactID: "f", Currency: "USD", ExpectedActiveSubscriptions: &n, MRRAmount: "5.00"}
		}()},
		{"baseline with subscriptionId", func() Revenue {
			n := 3
			return Revenue{Version: 1, Kind: KindMRRBaselineComplete, FactID: "f", Currency: "USD", ExpectedActiveSubscriptions: &n, SubscriptionID: "s"}
		}()},
		{"baseline with customerId", func() Revenue {
			n := 3
			return Revenue{Version: 1, Kind: KindMRRBaselineComplete, FactID: "f", Currency: "USD", ExpectedActiveSubscriptions: &n, CustomerID: "c"}
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.revenue.Validate(); err == nil {
				t.Fatalf("%+v validated but should not", tc.revenue)
			}
		})
	}
}

func TestRevenueValidationAcceptsEdgeCases(t *testing.T) {
	negativeTx := "-" + strings.Repeat("9", 38)
	revenue, err := NewTransaction("fact-x", negativeTx, "GBP")
	if err != nil {
		t.Fatalf("38-digit negative amount rejected: %v", err)
	}
	if revenue.Amount != negativeTx {
		t.Errorf("amount = %q", revenue.Amount)
	}

	if _, err := NewTransaction("0.000000001", "0.000000001", "USD"); err != nil {
		t.Fatalf("9 fraction digits rejected: %v", err)
	}
	if _, err := NewTransaction("-0", "0", "CHF"); err != nil {
		t.Fatalf("integer zero amount rejected: %v", err)
	}
}

func TestValidateOnHandBuiltPayload(t *testing.T) {
	subs := 12
	valid := Revenue{Version: 1, Kind: KindMRRBaselineComplete, FactID: "hand-built", Currency: "SEK", ExpectedActiveSubscriptions: &subs}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate on valid payload: %v", err)
	}
}

func TestSerializePropsToStringValues(t *testing.T) {
	props := Props{
		"string":  "plain",
		"int":     42,
		"int64":   int64(-7),
		"float":   96.4,
		"bool":    true,
		"nil":     nil,
		"map":     map[string]any{"a": 1},
		"slice":   []string{"x", "y"},
		"channel": make(chan int),
	}
	got, err := serializeProps(props)
	if err != nil {
		t.Fatalf("serializeProps: %v", err)
	}

	if len(got) != len(props)-1 {
		t.Fatalf("serializeProps produced %d keys (%v), want %d with nil skipped", len(got), got, len(props)-1)
	}
	checks := map[string]string{
		"string": "plain",
		"int":    "42",
		"int64":  "-7",
		"float":  "96.4",
		"bool":   "true",
		"map":    `{"a":1}`,
		"slice":  `["x","y"]`,
	}
	for key, want := range checks {
		if got[key] != want {
			t.Errorf("props[%q] = %q, want %q", key, got[key], want)
		}
	}
	if got["channel"] == "" {
		t.Errorf("unmarshalable value fell through empty; want a fmt fallback")
	}
}

func TestGenerateIDUniquenessAndShape(t *testing.T) {
	seen := make(map[string]bool, 500)
	for i := 0; i < 500; i++ {
		id := generateID()
		if len(id) != 32 {
			t.Fatalf("generateID length = %d, want 32 hex chars", len(id))
		}
		for _, c := range id {
			if !strings.ContainsRune("0123456789abcdef", c) {
				t.Fatalf("generateID produced non-hex char %q in %q", c, id)
			}
		}
		if seen[id] {
			t.Fatalf("generateID repeated %q within 500 draws", id)
		}
		seen[id] = true
	}
}
