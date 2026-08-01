package gateway

import (
	"context"
	"sync"
)

// MockTransport implements Transport and records every SendBatch call.
// It is safe for concurrent use and is intended for use in tests.
type MockTransport struct {
	mu    sync.Mutex
	calls []TransportCall
	// Resp is the response to return from SendBatch (nil returns nil, nil).
	Resp *IngestResponse
	// Err is the error to return from SendBatch.
	Err error
}

// TransportCall records the arguments of a single SendBatch invocation.
type TransportCall struct {
	Ctx context.Context
	Req *IngestRequest
}

// NewMockTransport creates a MockTransport that returns the given response on
// each SendBatch call.
func NewMockTransport(resp *IngestResponse) *MockTransport {
	return &MockTransport{Resp: resp}
}

// SendBatch implements Transport. It records the call and returns the
// configured Resp and Err.
func (m *MockTransport) SendBatch(ctx context.Context, req *IngestRequest) (*IngestResponse, error) {
	m.mu.Lock()
	m.calls = append(m.calls, TransportCall{Ctx: ctx, Req: req})
	m.mu.Unlock()
	if m.Err != nil {
		return nil, m.Err
	}
	return m.Resp, nil
}

// CallCount returns the number of times SendBatch was called.
func (m *MockTransport) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

// LastCall returns the most recent TransportCall, or nil if no calls.
func (m *MockTransport) LastCall() *TransportCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return nil
	}
	call := m.calls[len(m.calls)-1]
	return &call
}

// Calls returns a copy of all recorded TransportCall entries in order.
func (m *MockTransport) Calls() []TransportCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]TransportCall, len(m.calls))
	copy(out, m.calls)
	return out
}
