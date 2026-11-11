package syncer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PanelClient talks to the PasarGuard REST API.
type PanelClient struct {
	BaseURL  string
	Username string
	Password string
	HTTP     *http.Client
	PageSize int

	mu           sync.Mutex
	token        string
	legacyRoutes atomic.Bool // panel only has username-based user routes
}

// APIError is a non-2xx answer from PasarGuard.
type APIError struct {
	Method string
	Path   string
	Status int
	Body   string
}

func (e *APIError) Error() string {
	body := strings.TrimSpace(e.Body)
	if len(body) > 300 {
		body = body[:300] + "..."
	}
	return fmt.Sprintf("pasarguard %s %s: HTTP %d: %s", e.Method, e.Path, e.Status, body)
}

// NewPanelClient builds a client with sane defaults.
func NewPanelClient(baseURL, username, password string) *PanelClient {
	return &PanelClient{
		BaseURL:  strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		Username: username,
		Password: password,
		HTTP:     &http.Client{Timeout: 30 * time.Second},
		PageSize: 500,
	}
}

// Login fetches an admin access token.
func (c *PanelClient) Login(ctx context.Context) error {
	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("username", c.Username)
	form.Set("password", c.Password)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/admin/token", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("pasarguard login: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return &APIError{Method: http.MethodPost, Path: "/api/admin/token", Status: resp.StatusCode, Body: string(body)}
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &out); err != nil || out.AccessToken == "" {
		return fmt.Errorf("pasarguard login: no access_token in response")
	}
	c.mu.Lock()
	c.token = out.AccessToken
	c.mu.Unlock()
	return nil
}

func (c *PanelClient) currentToken() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.token
}

func (c *PanelClient) do(ctx context.Context, method, path string, payload any, out any) error {
	if c.currentToken() == "" {
		if err := c.Login(ctx); err != nil {
			return err
		}
	}
	var raw []byte
	if payload != nil {
		var err error
		if raw, err = json.Marshal(payload); err != nil {
			return err
		}
	}
	for attempt := 0; ; attempt++ {
		var body io.Reader
		if raw != nil {
			body = bytes.NewReader(raw)
		}
		req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		if raw != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("Authorization", "Bearer "+c.currentToken())
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return fmt.Errorf("pasarguard %s %s: %w", method, path, err)
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 {
			if err := c.Login(ctx); err != nil {
				return err
			}
			continue
		}
		if resp.StatusCode/100 != 2 {
			return &APIError{Method: method, Path: path, Status: resp.StatusCode, Body: string(data)}
		}
		if out != nil && len(bytes.TrimSpace(data)) > 0 {
			if err := json.Unmarshal(data, out); err != nil {
				return fmt.Errorf("pasarguard %s %s: decode: %w", method, path, err)
			}
		}
		return nil
	}
}

type panelUserJSON struct {
	ID            int64           `json:"id"`
	Username      string          `json:"username"`
	Status        string          `json:"status"`
	UsedTraffic   json.Number     `json:"used_traffic"`
	DataLimit     json.Number     `json:"data_limit"`
	Expire        json.RawMessage `json:"expire"`
	ProxySettings *struct {
		Vless *struct {
			ID string `json:"id"`
		} `json:"vless"`
	} `json:"proxy_settings"`
	GroupIDs []int64 `json:"group_ids"`
}

func (u panelUserJSON) toPanelUser() PanelUser {
	out := PanelUser{
		ID:          u.ID,
		Username:    u.Username,
		Status:      strings.ToLower(u.Status),
		UsedTraffic: numberToInt64(u.UsedTraffic),
		DataLimit:   numberToInt64(u.DataLimit),
		Expire:      parseExpire(u.Expire),
		GroupIDs:    u.GroupIDs,
	}
	if u.ProxySettings != nil && u.ProxySettings.Vless != nil {
		out.VlessID = u.ProxySettings.Vless.ID
	}
	return out
}

func numberToInt64(n json.Number) int64 {
	if n == "" {
		return 0
	}
	if v, err := n.Int64(); err == nil {
		return v
	}
	if f, err := n.Float64(); err == nil {
		return int64(f)
	}
	return 0
}

// parseExpire accepts null, unix seconds, or an ISO datetime (assumed UTC
// when it has no zone) and returns unix seconds (0 = never).
func parseExpire(raw json.RawMessage) int64 {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return 0
	}
	if strings.HasPrefix(text, "\"") {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return 0
		}
		s = strings.TrimSpace(s)
		if s == "" {
			return 0
		}
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return v
		}
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05", "2006-01-02 15:04:05"} {
			if t, err := time.Parse(layout, s); err == nil {
				return t.Unix()
			}
		}
		return 0
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil || f <= 0 {
		return 0
	}
	if f > 1e12 { // milliseconds
		f /= 1000
	}
	return int64(f)
}

func formatExpire(unix int64) any {
	if unix <= 0 {
		return 0
	}
	return time.Unix(unix, 0).UTC().Format("2006-01-02T15:04:05Z")
}

// ListUsers pages through every PasarGuard user.
func (c *PanelClient) ListUsers(ctx context.Context) ([]PanelUser, error) {
	pageSize := c.PageSize
	if pageSize <= 0 {
		pageSize = 500
	}
	var all []PanelUser
	for offset := 0; ; offset += pageSize {
		var page struct {
			Users []panelUserJSON `json:"users"`
			Total int             `json:"total"`
		}
		path := fmt.Sprintf("/api/users?offset=%d&limit=%d", offset, pageSize)
		if err := c.do(ctx, http.MethodGet, path, nil, &page); err != nil {
			return nil, err
		}
		for _, u := range page.Users {
			all = append(all, u.toPanelUser())
		}
		if len(page.Users) < pageSize || (page.Total > 0 && offset+pageSize >= page.Total) {
			return all, nil
		}
	}
}

// GroupIDs returns the id of every group on the panel.
func (c *PanelClient) GroupIDs(ctx context.Context) ([]int64, error) {
	var lastErr error
	for _, path := range []string{"/api/groups", "/api/groups/simple"} {
		var raw json.RawMessage
		if err := c.do(ctx, http.MethodGet, path, nil, &raw); err != nil {
			lastErr = err
			continue
		}
		ids, err := decodeGroupIDs(raw)
		if err != nil {
			lastErr = err
			continue
		}
		return ids, nil
	}
	return nil, lastErr
}

func decodeGroupIDs(raw json.RawMessage) ([]int64, error) {
	type group struct {
		ID int64 `json:"id"`
	}
	var list []group
	if err := json.Unmarshal(raw, &list); err != nil {
		var wrapped struct {
			Groups []group `json:"groups"`
		}
		if err := json.Unmarshal(raw, &wrapped); err != nil {
			return nil, fmt.Errorf("pasarguard groups: unexpected response")
		}
		list = wrapped.Groups
	}
	ids := make([]int64, 0, len(list))
	for _, g := range list {
		if g.ID > 0 {
			ids = append(ids, g.ID)
		}
	}
	return ids, nil
}

func userPayload(in UserInput, create bool) map[string]any {
	payload := map[string]any{}
	if create {
		payload["username"] = in.Username
		payload["data_limit_reset_strategy"] = "no_reset"
		payload["note"] = "synced from Rebecca"
	}
	if in.Status != "" {
		payload["status"] = in.Status
	}
	if in.DataLimit >= 0 {
		payload["data_limit"] = in.DataLimit
	}
	if in.Expire >= 0 {
		payload["expire"] = formatExpire(in.Expire)
	}
	if in.GroupIDs != nil {
		payload["group_ids"] = in.GroupIDs
	}
	if in.UUID != "" {
		payload["proxy_settings"] = map[string]any{
			"vless": map[string]any{"id": in.UUID},
			"vmess": map[string]any{"id": in.UUID},
		}
	}
	return payload
}

// CreateUser creates a user (always created active; callers disable after).
func (c *PanelClient) CreateUser(ctx context.Context, in UserInput) (PanelUser, error) {
	payload := userPayload(in, true)
	payload["status"] = "active"
	var out panelUserJSON
	if err := c.do(ctx, http.MethodPost, "/api/user", payload, &out); err != nil {
		return PanelUser{}, err
	}
	user := out.toPanelUser()
	if user.Username == "" {
		user.Username = in.Username
	}
	return user, nil
}

// routeMissing tells "this panel has no such route" apart from "user not found".
func routeMissing(err error) bool {
	apiErr, ok := err.(*APIError)
	if !ok {
		return false
	}
	if apiErr.Status == http.StatusMethodNotAllowed {
		return true
	}
	if apiErr.Status != http.StatusNotFound {
		return false
	}
	var body struct {
		Detail any `json:"detail"`
	}
	if json.Unmarshal([]byte(apiErr.Body), &body) != nil {
		return false
	}
	detail, _ := body.Detail.(string)
	return detail == "Not Found"
}

func (c *PanelClient) userPath(user PanelUser, suffix string) string {
	if c.legacyRoutes.Load() || user.ID <= 0 {
		return "/api/user/" + url.PathEscape(user.Username) + suffix
	}
	return "/api/user/by-id/" + strconv.FormatInt(user.ID, 10) + suffix
}

func (c *PanelClient) userCall(ctx context.Context, method string, user PanelUser, suffix string, payload any) error {
	err := c.do(ctx, method, c.userPath(user, suffix), payload, nil)
	if err != nil && !c.legacyRoutes.Load() && user.Username != "" && routeMissing(err) {
		c.legacyRoutes.Store(true)
		err = c.do(ctx, method, c.userPath(user, suffix), payload, nil)
	}
	return err
}

// ModifyUser updates status/limit/expire/groups/uuid of an existing user.
func (c *PanelClient) ModifyUser(ctx context.Context, user PanelUser, in UserInput) error {
	return c.userCall(ctx, http.MethodPut, user, "", userPayload(in, false))
}

// ResetUsage zeroes the user's traffic on PasarGuard.
func (c *PanelClient) ResetUsage(ctx context.Context, user PanelUser) error {
	return c.userCall(ctx, http.MethodPost, user, "/reset", nil)
}
