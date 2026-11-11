package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type fakeServer struct {
	mu          sync.Mutex
	logins      int
	expireToken bool // first authenticated call returns 401 once
	legacy      bool // panel without /api/user/by-id routes
	calls       []string
	bodies      []map[string]any
	users       int
}

func (f *fakeServer) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls = append(f.calls, r.Method+" "+r.URL.RequestURI())
		if r.URL.Path == "/api/admin/token" {
			_ = r.ParseForm()
			if r.Form.Get("username") != "admin" || r.Form.Get("password") != "secret" {
				w.WriteHeader(http.StatusUnauthorized)
				fmt.Fprint(w, `{"detail":"Incorrect username or password"}`)
				return
			}
			f.logins++
			fmt.Fprintf(w, `{"access_token":"tok%d","token_type":"bearer"}`, f.logins)
			return
		}
		if r.Header.Get("Authorization") != fmt.Sprintf("Bearer tok%d", f.logins) || f.expireToken {
			f.expireToken = false
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"detail":"Not authenticated"}`)
			return
		}
		if body, _ := io.ReadAll(r.Body); len(body) > 0 {
			var m map[string]any
			_ = json.Unmarshal(body, &m)
			f.bodies = append(f.bodies, m)
		}
		switch {
		case r.URL.Path == "/api/users":
			offset := r.URL.Query().Get("offset")
			var users []string
			start := 0
			fmt.Sscan(offset, &start)
			for i := start; i < start+2 && i < f.users; i++ {
				users = append(users, fmt.Sprintf(`{"id":%d,"username":"u%d","status":"active","used_traffic":%d,"data_limit":null,"expire":"2030-03-17T17:46:40","proxy_settings":{"vless":{"id":"11111111-2222-3333-4444-55555555555%d"}},"group_ids":[1,2]}`, i+1, i, int64(i)*1000, i))
			}
			fmt.Fprintf(w, `{"users":[%s],"total":%d}`, strings.Join(users, ","), f.users)
		case r.URL.Path == "/api/groups":
			fmt.Fprint(w, `{"groups":[{"id":1,"name":"a"},{"id":4,"name":"b"}],"total":2}`)
		case r.URL.Path == "/api/user" && r.Method == http.MethodPost:
			fmt.Fprint(w, `{"id":42,"username":"alice","status":"active","used_traffic":0,"expire":null}`)
		case strings.HasPrefix(r.URL.Path, "/api/user/by-id/"):
			if f.legacy {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"detail":"Not Found"}`)
				return
			}
			fmt.Fprint(w, `{}`)
		case strings.HasPrefix(r.URL.Path, "/api/user/"):
			fmt.Fprint(w, `{}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"detail":"Not Found"}`)
		}
	})
}

func newTestClient(t *testing.T, f *fakeServer) *PanelClient {
	srv := httptest.NewServer(f.handler(t))
	t.Cleanup(srv.Close)
	c := NewPanelClient(srv.URL+"/", "admin", "secret")
	c.PageSize = 2
	return c
}

func TestLoginWrongPassword(t *testing.T) {
	f := &fakeServer{}
	c := newTestClient(t, f)
	c.Password = "bad"
	if err := c.Login(context.Background()); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("err = %v", err)
	}
}

func TestListUsersPaginatesAndParses(t *testing.T) {
	f := &fakeServer{users: 5}
	c := newTestClient(t, f)
	users, err := c.ListUsers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 5 {
		t.Fatalf("got %d users", len(users))
	}
	u := users[3]
	if u.ID != 4 || u.Username != "u3" || u.UsedTraffic != 3000 || u.DataLimit != 0 || u.Expire != 1900000000 ||
		u.VlessID != "11111111-2222-3333-4444-555555555553" || !sameIDSet(u.GroupIDs, []int64{1, 2}) {
		t.Fatalf("parsed %+v", u)
	}
}

func TestReloginOn401(t *testing.T) {
	f := &fakeServer{users: 1}
	c := newTestClient(t, f)
	if _, err := c.ListUsers(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.expireToken = true
	if _, err := c.ListUsers(context.Background()); err != nil {
		t.Fatalf("relogin failed: %v", err)
	}
	if f.logins != 2 {
		t.Fatalf("logins = %d", f.logins)
	}
}

func TestGroupIDs(t *testing.T) {
	c := newTestClient(t, &fakeServer{})
	ids, err := c.GroupIDs(context.Background())
	if err != nil || !sameIDSet(ids, []int64{1, 4}) {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	if ids, err := decodeGroupIDs(json.RawMessage(`[{"id":7,"name":"x"}]`)); err != nil || len(ids) != 1 || ids[0] != 7 {
		t.Fatalf("plain list: %v %v", ids, err)
	}
}

func TestCreateAndModifyPayloads(t *testing.T) {
	f := &fakeServer{}
	c := newTestClient(t, f)
	created, err := c.CreateUser(context.Background(), UserInput{Username: "alice", UUID: "11111111-2222-3333-4444-555555555555",
		Status: "disabled", DataLimit: 300 << 30, Expire: 1900000000, GroupIDs: []int64{1, 4}})
	if err != nil || created.ID != 42 {
		t.Fatalf("create: %+v %v", created, err)
	}
	body := f.bodies[0]
	vless := body["proxy_settings"].(map[string]any)["vless"].(map[string]any)
	if body["username"] != "alice" || body["status"] != "active" || body["data_limit"] != float64(300<<30) ||
		body["expire"] != "2030-03-17T17:46:40Z" || vless["id"] != "11111111-2222-3333-4444-555555555555" ||
		len(body["group_ids"].([]any)) != 2 {
		t.Fatalf("create body = %v", body)
	}
	if err := c.ModifyUser(context.Background(), created, UserInput{Status: "disabled", DataLimit: -1, Expire: -1}); err != nil {
		t.Fatal(err)
	}
	mod := f.bodies[1]
	if mod["status"] != "disabled" || mod["data_limit"] != nil || mod["expire"] != nil || mod["username"] != nil || mod["proxy_settings"] != nil {
		t.Fatalf("partial modify body = %v", mod)
	}
	if err := c.ModifyUser(context.Background(), created, UserInput{Status: "active", DataLimit: 1, Expire: 0}); err != nil {
		t.Fatal(err)
	}
	if f.bodies[2]["expire"] != float64(0) {
		t.Fatalf("unlimited expire should be 0, got %v", f.bodies[2]["expire"])
	}
	last := f.calls[len(f.calls)-1]
	if last != "PUT /api/user/by-id/42" {
		t.Fatalf("modify route = %s", last)
	}
}

func TestLegacyRouteFallback(t *testing.T) {
	f := &fakeServer{legacy: true}
	c := newTestClient(t, f)
	user := PanelUser{ID: 42, Username: "alice"}
	if err := c.ResetUsage(context.Background(), user); err != nil {
		t.Fatal(err)
	}
	if got := f.calls[len(f.calls)-1]; got != "POST /api/user/alice/reset" {
		t.Fatalf("fallback route = %s", got)
	}
	if err := c.ModifyUser(context.Background(), user, UserInput{Status: "active", DataLimit: -1, Expire: -1}); err != nil {
		t.Fatal(err)
	}
	if got := f.calls[len(f.calls)-1]; got != "PUT /api/user/alice" {
		t.Fatalf("legacy mode not remembered: %s", got)
	}
}

func TestUserNotFoundIsNotTreatedAsMissingRoute(t *testing.T) {
	err := &APIError{Status: 404, Body: `{"detail":"User not found"}`}
	if routeMissing(err) {
		t.Fatal("user-not-found must not switch to legacy routes")
	}
	if !routeMissing(&APIError{Status: 404, Body: `{"detail":"Not Found"}`}) {
		t.Fatal("missing route not detected")
	}
}

func TestParseExpire(t *testing.T) {
	cases := map[string]int64{
		`null`:                         0,
		`0`:                            0,
		`1900000000`:                   1900000000,
		`1900000000000`:                1900000000,
		`"2030-03-17T17:46:40"`:        1900000000,
		`"2030-03-17T17:46:40Z"`:       1900000000,
		`"2030-03-17T21:16:40+03:30"`:  1900000000,
		`"2030-03-17T17:46:40.123456"`: 1900000000,
		`""`:                           0,
	}
	for in, want := range cases {
		if got := parseExpire(json.RawMessage(in)); got != want {
			t.Errorf("parseExpire(%s) = %d, want %d", in, got, want)
		}
	}
}

func TestDeriveUUID(t *testing.T) {
	if got := DeriveUUID(`{"id":"11111111-2222-3333-4444-555555555555","flow":""}`, "aaaaaaaabbbbccccddddeeeeeeeeeeee"); got != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("settings id: %s", got)
	}
	if got := DeriveUUID(`{}`, "AAAAAAAABBBBCCCCDDDDEEEEEEEEEEEE"); got != "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" {
		t.Fatalf("credential key: %s", got)
	}
	if got := DeriveUUID(`{"id":"not-a-uuid"}`, ""); got != "" {
		t.Fatalf("invalid: %s", got)
	}
	if got := DeriveUUID("", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"); got != "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" {
		t.Fatalf("dashed key: %s", got)
	}
}
