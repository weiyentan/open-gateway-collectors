package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// defaultMaxRetries is the default number of retry attempts after the
	// initial request (total attempts = maxRetries + 1).
	defaultMaxRetries = 3

	// defaultMaxBackoff is the ceiling for exponential backoff.
	defaultMaxBackoff = 30 * time.Second

	// defaultBaseBackoff is the initial backoff duration before the first retry.
	defaultBaseBackoff = 1 * time.Second

	// defaultHTTPTimeout is the timeout for the underlying HTTP client.
	defaultHTTPTimeout = 30 * time.Second
)

// Client communicates with the OpenCode Gateway's /ingest endpoint.
// Create one via NewClient and call SendBatch to push usage records.
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
	logger     *slog.Logger
	hostname   string

	// Testing hooks — unexported so normal code uses defaults, but tests in
	// the same package can override them.
	maxRetries  int
	maxBackoff  time.Duration
	baseBackoff time.Duration
}

// NewClient creates a new Client with the given Gateway base URL, bearer
// token, and client hostname. The hostname is attached to every outgoing
// IngestRequest by SendBatch.
func NewClient(baseURL, token, hostname string) *Client {
	return &Client{
		baseURL:     strings.TrimRight(baseURL, "/"),
		token:       token,
		httpClient:  &http.Client{Timeout: defaultHTTPTimeout},
		logger:      slog.Default(),
		hostname:    hostname,
		maxRetries:  defaultMaxRetries,
		maxBackoff:  defaultMaxBackoff,
		baseBackoff: defaultBaseBackoff,
	}
}

// rawResponse holds the unparsed HTTP response from the Gateway.
type rawResponse struct {
	statusCode int
	body       []byte
}

// doRequest performs a single HTTP POST to {baseURL}/ingest with the
// serialized JSON body. It returns the raw response or an error from the
// underlying HTTP round-trip.
func (c *Client) doRequest(ctx context.Context, reqBody []byte) (*rawResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/ingest", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.token)

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http do: %w", err)
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return &rawResponse{statusCode: httpResp.StatusCode, body: body}, nil
}

// isRetryableError returns true if the error is a network-level error
// (connection refused, DNS failure, timeout) but NOT context cancellation
// or deadline exceeded.
func isRetryableError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// nextBackoff doubles the current backoff, capping at maxBackoff.
func (c *Client) nextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > c.maxBackoff {
		next = c.maxBackoff
	}
	return next
}

// SendBatch serialises req, sets ClientHostname from the stored hostname,
// and POSTs it to the Gateway. It retries on connection errors, 5xx, and
// timeouts with exponential backoff (±25% jitter). It does NOT retry on
// 4xx status codes or context cancellation.
func (c *Client) SendBatch(ctx context.Context, req *IngestRequest) (*IngestResponse, error) {
	req.ClientHostname = c.hostname

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	var lastErr error
	backoff := c.baseBackoff

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			// Jitter: ±25% of the current backoff duration.
			jitter := time.Duration(float64(backoff) * (0.75 + rand.Float64()*0.5))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(jitter):
			}
		}

		raw, err := c.doRequest(ctx, body)
		if err != nil {
			// Network-level / transport error.
			lastErr = err
			if isRetryableError(err) && attempt < c.maxRetries {
				backoff = c.nextBackoff(backoff)
				c.logger.Warn("gateway request failed, retrying",
					"attempt", attempt+1,
					"max_retries", c.maxRetries,
					"backoff", backoff,
					"error", err,
				)
				continue
			}
			if isRetryableError(err) {
				c.logger.Error("gateway request failed after max retries",
					"error", err,
				)
				return nil, lastErr
			}
			// Non-retryable transport error (context cancelled / deadline exceeded).
			return nil, err
		}

		// Successful 2xx response.
		if raw.statusCode >= 200 && raw.statusCode < 300 {
			var ingestResp IngestResponse
			if err := json.Unmarshal(raw.body, &ingestResp); err != nil {
				return nil, fmt.Errorf("parse response: %w", err)
			}
			return &ingestResp, nil
		}

		// Server error — retry if attempts remain.
		if raw.statusCode >= 500 {
			lastErr = fmt.Errorf("gateway returned status %d", raw.statusCode)
			if attempt < c.maxRetries {
				backoff = c.nextBackoff(backoff)
				c.logger.Warn("gateway returned server error, retrying",
					"attempt", attempt+1,
					"max_retries", c.maxRetries,
					"status", raw.statusCode,
					"backoff", backoff,
				)
				continue
			}
			c.logger.Error("gateway request failed after max retries",
				"status", raw.statusCode,
			)
			return nil, lastErr
		}

		// 4xx (or any other non-2xx, non-5xx) — not retryable.
		return nil, fmt.Errorf("gateway returned non-retryable status %d: %s", raw.statusCode, string(raw.body))
	}

	return nil, lastErr
}

// MapToIngestRecord converts an internal UsageRecord to the wire-format
// IngestRecord. CachedTokens is the sum of TokensCacheRead and
// TokensCacheWrite. EstimatedCostUSD is formatted as a decimal string, or
// nil if the cost is zero. ReportedAt is an ISO 8601 string derived from
// OccurredAt.
func MapToIngestRecord(record UsageRecord) IngestRecord {
	cachedTokens := record.TokensCacheRead + record.TokensCacheWrite

	var estimatedCostUSD *string
	if record.EstimatedCostUSD != 0 {
		s := strconv.FormatFloat(record.EstimatedCostUSD, 'f', -1, 64)
		estimatedCostUSD = &s
	}

	return IngestRecord{
		SourceRecordID:   record.SourceRecordID,
		SessionID:        record.SessionID,
		Model:            record.Model,
		Provider:         record.ProviderID,
		Mode:             record.Mode,
		InputTokens:      record.InputTokens,
		OutputTokens:     record.OutputTokens,
		CachedTokens:     cachedTokens,
		EstimatedCostUSD: estimatedCostUSD,
		ReportedAt:       record.OccurredAt.Format(time.RFC3339),
	}
}
