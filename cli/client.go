package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var mainCtx = context.Background()

// ---- configuration ---------------------------------------------------------------------------------------------------

type Config struct {
	URL      string `json:"url"`
	Token    string `json:"token"`
	Passcode string `json:"passcode,omitempty"`
}

func configPath() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		dir, _ = os.UserHomeDir()
	}
	return filepath.Join(dir, "bifrost", "config.json")
}
func loadConfig() Config {
	c := Config{URL: "https://bifrost.kineuro.se"}
	if b, err := os.ReadFile(configPath()); err == nil {
		_ = json.Unmarshal(b, &c)
	}
	if v := os.Getenv("BIFROST_URL"); v != "" {
		c.URL = v
	}
	if v := os.Getenv("BIFROST_TOKEN"); v != "" {
		c.Token = v
	}
	if v := os.Getenv("BIFROST_PASSCODE"); v != "" {
		c.Passcode = v
	}
	c.URL = strings.TrimRight(c.URL, "/")
	return c
}
func saveConfig(c Config) error {
	p := configPath()
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(c, "", "  ")
	return os.WriteFile(p, b, 0o600)
}

// ---- HTTP client --------------------------------------------------------------------------------------------------------

type Client struct {
	cfg  Config
	http *http.Client
}

func newClient() (*Client, error) {
	cfg := loadConfig()
	if cfg.Token == "" {
		return nil, errors.New("no token: run `bifrost login <token>` or set BIFROST_TOKEN")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 32
	tr.MaxConnsPerHost = 0
	tr.ResponseHeaderTimeout = 5 * time.Minute
	tr.IdleConnTimeout = 90 * time.Second
	return &Client{cfg: cfg, http: &http.Client{Transport: tr}}, nil
}

type apiError struct {
	Status   int
	Message  string
	Passcode bool
	Retry    time.Duration
}

func (e *apiError) Error() string { return fmt.Sprintf("%s (HTTP %d)", e.Message, e.Status) }

func (c *Client) req(ctx context.Context, method, path string, body io.Reader, hdr map[string]string) (*http.Response, error) {
	r, err := http.NewRequestWithContext(ctx, method, c.cfg.URL+path, body)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	r.Header.Set("User-Agent", "bifrost-cli/"+version+" ("+runtime.GOOS+"/"+runtime.GOARCH+")")
	if c.cfg.Passcode != "" {
		r.Header.Set("X-Bifrost-Passcode", c.cfg.Passcode)
	}
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	res, err := c.http.Do(r)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 400 {
		defer res.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		e := &apiError{Status: res.StatusCode, Message: strings.TrimSpace(string(b))}
		var j struct {
			Error    string `json:"error"`
			Passcode bool   `json:"passcode"`
		}
		if json.Unmarshal(b, &j) == nil && j.Error != "" {
			e.Message, e.Passcode = j.Error, j.Passcode
		}
		if ra := res.Header.Get("Retry-After"); ra != "" {
			if n, err := strconv.Atoi(ra); err == nil {
				e.Retry = time.Duration(n) * time.Second
			}
		}
		return nil, e
	}
	return res, nil
}

func (c *Client) json(ctx context.Context, method, path string, in any, out any) error {
	var body io.Reader
	hdr := map[string]string{}
	if in != nil {
		b, _ := json.Marshal(in)
		body = bytes.NewReader(b)
		hdr["Content-Type"] = "application/json"
	}
	res, err := c.req(ctx, method, path, body, hdr)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if out == nil {
		_, _ = io.Copy(io.Discard, res.Body)
		return nil
	}
	return json.NewDecoder(res.Body).Decode(out)
}

// A server that is not there is not a server that said no. A connection that will not open, or the 502 and 504
// the gateway answers with while the container behind it is being replaced, means "not yet". Waiting those out
// on their own budget is what lets a restart pass underneath a running transfer instead of ending it, which is
// the whole point of a resumable push. The budget is deliberately longer than a rebuild and restart take.
const unreachableBudget = 5 * time.Minute

// unreachable reports whether err means the server never answered, as opposed to answering with a refusal.
func unreachable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var ae *apiError
	if errors.As(err, &ae) {
		return ae.Status == 502 || ae.Status == 504
	}
	return true // no HTTP reply came back at all: refused, reset, or cut off mid-answer
}

// retry runs fn until it succeeds, giving up after attempts; 503 "busy" waits for Retry-After, a server that is
// away waits for it to come back, other errors back off.
func retry(ctx context.Context, attempts int, fn func() error) error {
	var last error
	var awaySince time.Time
	for i := 0; i < attempts; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := fn()
		if err == nil {
			return nil
		}
		last = err
		var ae *apiError
		if errors.As(err, &ae) {
			if ae.Status == 401 || ae.Status == 403 || ae.Status == 404 || ae.Status == 400 || ae.Status == 507 {
				return err // no point retrying
			}
			if ae.Status == 503 {
				d := ae.Retry
				if d == 0 {
					d = 3 * time.Second
				}
				busy(d)
				select {
				case <-time.After(d):
				case <-ctx.Done():
					return ctx.Err()
				}
				i-- // being told to wait is not a failure
				continue
			}
		}
		if unreachable(err) {
			if awaySince.IsZero() {
				awaySince = time.Now()
			}
			if time.Since(awaySince) < unreachableBudget {
				const d = 5 * time.Second
				busy(d) // hold every worker off, so a server coming back is not met by all of them at once
				select {
				case <-time.After(d):
				case <-ctx.Done():
					return ctx.Err()
				}
				i-- // waiting for the server to come back is not an attempt spent
				continue
			}
		} else {
			awaySince = time.Time{}
		}
		d := time.Duration(1<<uint(i)) * time.Second
		if d > 30*time.Second {
			d = 30 * time.Second
		}
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return last
}

// ---- share view --------------------------------------------------------------------------------------------------------

type Limits struct {
	PartSize   int64 `json:"part_size"`
	LargeFile  int64 `json:"large_file"`
	BatchBytes int64 `json:"batch_bytes"`
	BatchFiles int   `json:"batch_files"`
	Streams    int   `json:"streams"`
}
type Share struct {
	ID               string  `json:"id"`
	Name             string  `json:"name"`
	Partner          string  `json:"partner"`
	Direction        string  `json:"direction"`
	Message          string  `json:"message"`
	Status           string  `json:"status"`
	ExpiresAt        *string `json:"expires_at"`
	QuotaBytes       int64   `json:"quota_bytes"`
	MaxFiles         int64   `json:"max_files"`
	MaxDownloadBytes int64   `json:"max_download_bytes"`
	UsedBytes        int64   `json:"used_bytes"`
	Files            int64   `json:"files"`
	DownloadedBytes  int64   `json:"downloaded_bytes"`
	CanUpload        bool    `json:"can_upload"`
	CanDownload      bool    `json:"can_download"`
	Limits           Limits  `json:"limits"`
}
type shareResp struct {
	Share      Share `json:"share"`
	Credential struct {
		ID    string `json:"id"`
		Label string `json:"label"`
	} `json:"credential"`
}

func (c *Client) share(ctx context.Context) (*shareResp, error) {
	var s shareResp
	if err := retry(ctx, 5, func() error { return c.json(ctx, "GET", "/api/share", nil, &s) }); err != nil {
		return nil, err
	}
	return &s, nil
}

type Entry struct {
	Name   string  `json:"name"`
	Path   string  `json:"path"`
	Dir    bool    `json:"dir"`
	Size   int64   `json:"size"`
	Mtime  string  `json:"mtime"`
	Sha256 *string `json:"sha256"`
}

// ---- formatting --------------------------------------------------------------------------------------------------------

func fmtBytes(n int64) string {
	u := []string{"B", "kB", "MB", "GB", "TB", "PB"}
	v := float64(n)
	i := 0
	for v >= 1000 && i < len(u)-1 {
		v /= 1000
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d %s", n, u[i])
	}
	return fmt.Sprintf("%.1f %s", v, u[i])
}
func parseRate(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	mult := int64(1)
	switch strings.ToUpper(s[len(s)-1:]) {
	case "K":
		mult, s = 1000, s[:len(s)-1]
	case "M":
		mult, s = 1000_000, s[:len(s)-1]
	case "G":
		mult, s = 1000_000_000, s[:len(s)-1]
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("bad rate %q", s)
	}
	return int64(v * float64(mult)), nil
}
