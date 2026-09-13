// Package ssrf guards outbound webhook delivery against SSRF.
//
// Two layers:
//  1. Registration-time: ValidateURL rejects non-http(s) schemes, userinfo,
//     and any host resolving to loopback / private / link-local / reserved
//     addresses (this covers the machine's own admin ports on 127.0.0.0/8
//     and internal RFC1918/CGNAT ranges).
//  2. Delivery-time: DialContext re-resolves and re-checks the IP at connect
//     time, which closes DNS-rebinding races between validation and connect.
//
// Redirects are never followed by the delivery HTTP client (see dispatcher),
// so a validated URL cannot bounce the worker into a blocked address.
//
// Local receivers for tests/dev are only reachable through an explicit
// allowlist of hosts/CIDRs (e.g. "127.0.0.1/32").
package ssrf

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// Guard validates endpoint URLs and dials them safely.
type Guard struct {
	allowHosts map[string]struct{}
	allowCIDRs []*net.IPNet
	blocked    []*net.IPNet
	resolver   interface {
		LookupIP(ctx context.Context, network, host string) ([]net.IP, error)
	}
}

// New builds a Guard. allowlist entries are either CIDRs ("127.0.0.1/32") or
// host names ("localhost"); anything matching the allowlist bypasses the
// public-IP checks. Keep this list empty in production.
func New(allowlist []string) (*Guard, error) {
	g := &Guard{
		allowHosts: map[string]struct{}{},
		resolver:   net.DefaultResolver,
	}
	for _, cidr := range []string{
		"0.0.0.0/8",          // "this" network
		"100.64.0.0/10",      // CGNAT (RFC 6598)
		"192.0.0.0/24",       // IETF protocol assignments
		"192.0.2.0/24",       // TEST-NET-1 documentation
		"198.18.0.0/15",      // benchmarking
		"198.51.100.0/24",    // TEST-NET-2 documentation
		"203.0.113.0/24",     // TEST-NET-3 documentation
		"224.0.0.0/4",        // multicast
		"240.0.0.0/4",        // reserved
		"2001:db8::/32",      // IPv6 documentation
	} {
		_, n, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("ssrf: bad built-in CIDR %q: %w", cidr, err)
		}
		g.blocked = append(g.blocked, n)
	}
	for _, entry := range allowlist {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if _, n, err := net.ParseCIDR(entry); err == nil {
			g.allowCIDRs = append(g.allowCIDRs, n)
			continue
		}
		if ip := net.ParseIP(entry); ip != nil {
			bits := 32
			if ip.To4() == nil {
				bits = 128
			}
			g.allowCIDRs = append(g.allowCIDRs, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
			continue
		}
		g.allowHosts[strings.ToLower(entry)] = struct{}{}
	}
	return g, nil
}

// ValidateURL checks a webhook endpoint URL at registration time.
func (g *Guard) ValidateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme %q not allowed: only http and https", u.Scheme)
	}
	if u.User != nil {
		return fmt.Errorf("userinfo (credentials) in URL is not allowed")
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("URL must include a host")
	}
	if g.hostAllowed(host) {
		return nil
	}
	ips, err := g.lookup(context.Background(), host)
	if err != nil {
		return fmt.Errorf("cannot resolve host %q: %w", host, err)
	}
	if len(ips) == 0 {
		return fmt.Errorf("host %q resolves to no addresses", host)
	}
	for _, ip := range ips {
		if !g.ipAllowed(ip) {
			return fmt.Errorf("host %q resolves to non-public address %s", host, ip)
		}
	}
	return nil
}

// DialContext returns a DialContext for the delivery HTTP transport. It
// re-validates the resolved IP at connect time (DNS-rebinding defense) and
// dials the validated IP directly; TLS SNI/verification still uses the
// original hostname because http.Transport derives ServerName from the URL.
func (g *Guard) DialContext(base *net.Dialer) func(ctx context.Context, network, addr string) (net.Conn, error) {
	if base == nil {
		base = &net.Dialer{}
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("ssrf dial: %w", err)
		}
		if g.hostAllowed(host) {
			return base.DialContext(ctx, network, addr)
		}
		ips, err := g.lookup(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("ssrf dial: resolve %q: %w", host, err)
		}
		for _, ip := range ips {
			if !g.ipAllowed(ip) {
				return nil, fmt.Errorf("ssrf dial: %q resolved to blocked address %s", host, ip)
			}
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("ssrf dial: %q resolved to no addresses", host)
		}
		return base.DialContext(ctx, network, net.JoinHostPort(ips[0].String(), port))
	}
}

func (g *Guard) lookup(ctx context.Context, host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	return g.resolver.LookupIP(ctx, "ip", host)
}

func (g *Guard) hostAllowed(host string) bool {
	_, ok := g.allowHosts[strings.ToLower(host)]
	return ok
}

func (g *Guard) ipAllowed(ip net.IP) bool {
	for _, n := range g.allowCIDRs {
		if n.Contains(ip) {
			return true
		}
	}
	return isPublic(ip, g.blocked)
}

// isPublic reports whether ip is a globally routable unicast address.
func isPublic(ip net.IP, blocked []*net.IPNet) bool {
	if ip == nil {
		return false
	}
	if ip.IsUnspecified() || ip.IsLoopback() || ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() {
		return false
	}
	for _, n := range blocked {
		if n.Contains(ip) {
			return false
		}
	}
	return true
}
