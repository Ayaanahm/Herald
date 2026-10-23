package config

import (
	"testing"
	"time"
)

func TestParseTenants(t *testing.T) {
	got, err := ParseTenants("k1:acme:100/1s, k2:globex:5000/1m")
	if err != nil {
		t.Fatal(err)
	}
	if got["k1"].ID != "acme" || got["k1"].RateLimit != 100 || got["k1"].RateWindow != time.Second {
		t.Fatalf("k1: %+v", got["k1"])
	}
	if got["k2"].RateWindow != time.Minute {
		t.Fatalf("k2: %+v", got["k2"])
	}
	for _, bad := range []string{"", "k1:acme", "k1:acme:100", "k1:acme:x/1s", "k1:acme:0/1s", "k1:acme:10/nope"} {
		if _, err := ParseTenants(bad); err == nil {
			t.Errorf("%q: expected error", bad)
		}
	}
}
