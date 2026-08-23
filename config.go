package alitycs

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultEndpoint is the worker ingest endpoint events are POSTed to.
const DefaultEndpoint = "https://api.alitycs.com/events"

const (
	defaultFlushSize      = 25
	defaultFlushInterval  = 10 * time.Second
	defaultMaxQueueSize   = 1000
	defaultMaxRetries     = 3
	defaultSessionTimeout = 30 * time.Minute
	defaultHTTPTimeout    = 10 * time.Second
)

// config holds the resolved client configuration.
type config struct {
	apiKey         string
	endpoint       string
	flushSize      int
	flushInterval  time.Duration
	maxQueueSize   int
	maxRetries     int
	sessionTimeout time.Duration
	httpClient     *http.Client
	debug          bool
}

// ErrAPIKeyRequired is returned by New when the publishable key is empty.
var ErrAPIKeyRequired = errors.New("alitycs: apiKey is required")

// Option configures a Client. Options are applied by New.
type Option func(*config) error

// WithEndpoint overrides the ingest endpoint. Defaults to
// https://api.alitycs.com/events.
func WithEndpoint(endpoint string) Option {
	return func(c *config) error {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return errors.New("alitycs: invalid endpoint URL")
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return errors.New("alitycs: endpoint must be an http or https URL")
		}
		c.endpoint = strings.TrimRight(endpoint, "/")
		return nil
	}
}

// WithFlushSize sets how many buffered events trigger an automatic send.
// Must be positive; defaults to 25. A flush size of 1 sends every event on its
// own, effectively disabling coalescing.
func WithFlushSize(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return errors.New("alitycs: flush size must be at least 1")
		}
		c.flushSize = n
		return nil
	}
}

// WithFlushInterval sets how often partially filled batches are sent.
// A non-positive interval disables the timer entirely — events are then only
// sent by reaching the flush size or by Flush/Shutdown. Defaults to 10s.
func WithFlushInterval(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return errors.New("alitycs: flush interval must not be negative")
		}
		c.flushInterval = d
		return nil
	}
}

// WithMaxQueueSize bounds how many events may sit unsent before new ones are
// dropped. Must be positive; defaults to 1000.
func WithMaxQueueSize(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return errors.New("alitycs: max queue size must be at least 1")
		}
		c.maxQueueSize = n
		return nil
	}
}

// WithMaxRetries sets how many times a failed batch is retried. 5xx and 429
// responses and transport errors are retried with exponential backoff; other
// 4xx responses are terminal. Must be non-negative; defaults to 3.
func WithMaxRetries(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return errors.New("alitycs: max retries must not be negative")
		}
		c.maxRetries = n
		return nil
	}
}

// WithSessionTimeout sets the inactivity period after which the session ID
// rotates (the anonymous ID is kept). Must be positive; defaults to 30m.
func WithSessionTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return errors.New("alitycs: session timeout must be positive")
		}
		c.sessionTimeout = d
		return nil
	}
}

// WithHTTPClient injects a custom *http.Client, for proxies, transports or
// timeouts; it is used exactly as provided. The default client has a 10s
// timeout.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *config) error {
		if hc == nil {
			return errors.New("alitycs: http client must not be nil")
		}
		c.httpClient = hc
		return nil
	}
}

// WithDebug enables stderr diagnostics under the [Alitycs] prefix.
func WithDebug(debug bool) Option {
	return func(c *config) error {
		c.debug = debug
		return nil
	}
}

func newConfig(apiKey string, opts ...Option) (*config, error) {
	cfg := &config{
		apiKey:         apiKey,
		endpoint:       DefaultEndpoint,
		flushSize:      defaultFlushSize,
		flushInterval:  defaultFlushInterval,
		maxQueueSize:   defaultMaxQueueSize,
		maxRetries:     defaultMaxRetries,
		sessionTimeout: defaultSessionTimeout,
	}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func (c *config) httpClientOrDefault() *http.Client {
	if c.httpClient != nil {
		return c.httpClient
	}
	return &http.Client{Timeout: defaultHTTPTimeout}
}
