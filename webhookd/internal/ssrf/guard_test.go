package ssrf

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
)

func TestValidateURL(t *testing.T) {
	guard, err := New(nil) // strict: no allowlist
	if err != nil {
		t.Fatal(err)
	}

	blocked := []string{
		// loopback / local admin interfaces
		"http://127.0.0.1:8080/admin",
		"http://127.0.0.2/hook",
		"https://[::1]:9090/",
		"http://localhost:8080/hook",
		// RFC1918 private ranges
		"http://10.0.0.5/hook",
		"http://172.16.3.4/hook",
		"http://192.168.1.1/hook",
		// link-local (cloud metadata endpoints live here)
		"http://169.254.169.254/latest/meta-data",
		"http://[fe80::1]/hook",
		// CGNAT / reserved / unspecified
		"http://100.64.1.1/hook",
		"http://0.0.0.0/hook",
		"http://240.1.2.3/hook",
		"http://198.18.0.1/hook",
		// IPv6 ULA / multicast
		"http://[fd00::1]/hook",
		"http://[ff02::1]/hook",
		// non-http schemes
		"file:///etc/passwd",
		"gopher://example.com/",
		"ftp://example.com/hook",
		// credentials in URL
		"http://user:pass@example.com/hook",
		// no host
		"http:///path",
	}
	for _, u := range blocked {
		if err := guard.ValidateURL(u); err == nil {
			t.Errorf("ValidateURL(%q) = nil, want error", u)
		}
	}

	allowed := []string{
		// IP literals only: sandbox DNS may map names to reserved ranges.
		"https://93.184.216.34/hook",
		"http://8.8.8.8:8443/hook?token=abc",
	}
	for _, u := range allowed {
		if err := guard.ValidateURL(u); err != nil {
			t.Errorf("ValidateURL(%q) = %v, want nil", u, err)
		}
	}
}

// fakeResolver lets tests control DNS results.
type fakeResolver map[string][]net.IP

func (f fakeResolver) LookupIP(_ context.Context, _, host string) ([]net.IP, error) {
	if ips, ok := f[host]; ok {
		return ips, nil
	}
	return nil, fmt.Errorf("no such host")
}

func TestValidateURLWithDNS(t *testing.T) {
	guard, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	guard.resolver = fakeResolver{
		"good.example.com":  {net.ParseIP("93.184.216.34")},
		"sneaky.example.com": {net.ParseIP("93.184.216.34"), net.ParseIP("10.1.2.3")},
		"internal.example.com": {net.ParseIP("192.168.1.10")},
	}
	if err := guard.ValidateURL("https://good.example.com/hook"); err != nil {
		t.Errorf("public host rejected: %v", err)
	}
	// One private address among several public ones must fail closed.
	if err := guard.ValidateURL("https://sneaky.example.com/hook"); err == nil {
		t.Error("host with mixed public/private records accepted, want rejection")
	}
	if err := guard.ValidateURL("https://internal.example.com/hook"); err == nil {
		t.Error("internal host accepted, want rejection")
	}
}

func TestAllowlistPermitsLocalReceivers(t *testing.T) {
	guard, err := New([]string{"127.0.0.1/32", "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	// Explicitly allowed local receivers pass.
	for _, u := range []string{
		"http://127.0.0.1:9999/testkit/receive",
		"http://localhost:9999/hook",
	} {
		if err := guard.ValidateURL(u); err != nil {
			t.Errorf("allowlisted %q rejected: %v", u, err)
		}
	}
	// Other loopback addresses and private ranges stay blocked.
	for _, u := range []string{
		"http://127.0.0.2:9999/hook",
		"http://10.1.2.3/hook",
		"http://169.254.169.254/",
	} {
		if err := guard.ValidateURL(u); err == nil {
			t.Errorf("ValidateURL(%q) = nil, want error even with allowlist", u)
		}
	}
}

func TestDialContextBlocksRebinding(t *testing.T) {
	guard, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	dial := guard.DialContext(nil)
	_, err = dial(context.Background(), "tcp", "10.0.0.1:443")
	if err == nil || !strings.Contains(err.Error(), "blocked") {
		t.Errorf("dial to private IP: got %v, want blocked error", err)
	}
	_, err = dial(context.Background(), "tcp", "127.0.0.1:8080")
	if err == nil {
		t.Errorf("dial to loopback: got nil, want blocked error")
	}
}
