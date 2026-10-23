package provider

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func send(t *testing.T, h http.Handler, ch, key string, fence int64, recipient string) int {
	t.Helper()
	b, _ := json.Marshal(SendRequest{IdempotencyKey: key, Fence: fence, Recipient: recipient, Payload: json.RawMessage(`{}`)})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/send/"+ch, bytes.NewReader(b)))
	return rec.Code
}

func TestIdempotencyAndFencing(t *testing.T) {
	s := NewServer(map[string]ChannelConfig{"push": {}})
	h := s.Handler()
	if c := send(t, h, "push", "n1", 5, "u"); c != 200 {
		t.Fatalf("first send: %d", c)
	}
	if c := send(t, h, "push", "n1", 6, "u"); c != 200 {
		t.Fatalf("repeat with newer token: %d", c)
	}
	if c := send(t, h, "push", "n1", 3, "u"); c != 409 {
		t.Fatalf("older token: %d, want 409", c)
	}
	st := s.Stats()["push"]
	if st.Sent != 1 || st.Duplicates != 1 || st.StaleFenced != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestFaultsAndInvalidRecipients(t *testing.T) {
	s := NewServer(map[string]ChannelConfig{"push": {Down: true}, "sms": {}})
	h := s.Handler()
	if c := send(t, h, "push", "a", 1, "u"); c != 503 {
		t.Fatalf("down channel: %d", c)
	}
	if c := send(t, h, "sms", "b", 1, "invalid-sms-1"); c != 422 {
		t.Fatalf("invalid recipient: %d", c)
	}
	if c := send(t, h, "fax", "c", 1, "u"); c != 404 {
		t.Fatalf("unknown channel: %d", c)
	}
}
