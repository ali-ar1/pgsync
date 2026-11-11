package syncer

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

const gb = int64(1 << 30)

// ---- fakes -------------------------------------------------------------

type fakePanel struct {
	mu        sync.Mutex
	users     map[int64]*PanelUser
	nextID    int64
	groups    []int64
	rejectRaw map[string]bool // usernames PasarGuard refuses (422)
	creates   []UserInput
	modifies  []UserInput
	resets    []int64
}

func newFakePanel() *fakePanel {
	return &fakePanel{users: map[int64]*PanelUser{}, nextID: 100, groups: []int64{1, 2, 3}, rejectRaw: map[string]bool{}}
}

func (p *fakePanel) ListUsers(context.Context) ([]PanelUser, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]PanelUser, 0, len(p.users))
	for _, u := range p.users {
		cp := *u
		cp.GroupIDs = append([]int64(nil), u.GroupIDs...)
		out = append(out, cp)
	}
	return out, nil
}

func (p *fakePanel) GroupIDs(context.Context) ([]int64, error) { return p.groups, nil }

func (p *fakePanel) CreateUser(_ context.Context, in UserInput) (PanelUser, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.creates = append(p.creates, in)
	if p.rejectRaw[in.Username] {
		return PanelUser{}, &APIError{Status: http.StatusUnprocessableEntity, Body: `{"detail":"invalid username"}`}
	}
	for _, u := range p.users {
		if strings.EqualFold(u.Username, in.Username) {
			return PanelUser{}, &APIError{Status: http.StatusConflict, Body: `{"detail":"User already exists"}`}
		}
	}
	p.nextID++
	u := &PanelUser{ID: p.nextID, Username: in.Username, Status: "active", DataLimit: in.DataLimit,
		Expire: in.Expire, VlessID: in.UUID, GroupIDs: append([]int64(nil), in.GroupIDs...)}
	p.users[u.ID] = u
	return *u, nil
}

func (p *fakePanel) ModifyUser(_ context.Context, user PanelUser, in UserInput) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.modifies = append(p.modifies, in)
	u := p.users[user.ID]
	if u == nil {
		return &APIError{Status: http.StatusNotFound, Body: `{"detail":"User not found"}`}
	}
	if in.Status != "" {
		u.Status = in.Status
	}
	if in.DataLimit >= 0 {
		u.DataLimit = in.DataLimit
	}
	if in.Expire >= 0 {
		u.Expire = in.Expire
	}
	if in.GroupIDs != nil {
		u.GroupIDs = append([]int64(nil), in.GroupIDs...)
	}
	if in.UUID != "" {
		u.VlessID = in.UUID
	}
	return nil
}

func (p *fakePanel) ResetUsage(_ context.Context, user PanelUser) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resets = append(p.resets, user.ID)
	if u := p.users[user.ID]; u != nil {
		u.UsedTraffic = 0
		if u.Status == "limited" {
			u.Status = "active"
		}
	}
	return nil
}

func (p *fakePanel) byName(name string) *PanelUser {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, u := range p.users {
		if u.Username == name {
			return u
		}
	}
	return nil
}

func (p *fakePanel) use(name string, bytes int64) {
	u := p.byName(name)
	p.mu.Lock()
	u.UsedTraffic += bytes
	p.mu.Unlock()
}

type fakeRebecca struct {
	users   map[int64]*RebeccaUser
	addErr  error
	addCall int
}

func (r *fakeRebecca) ListUsers(context.Context) ([]RebeccaUser, error) {
	out := []RebeccaUser{}
	for id := int64(1); id <= 1000; id++ {
		if u, ok := r.users[id]; ok {
			out = append(out, *u)
		}
	}
	return out, nil
}

func (r *fakeRebecca) AddUsage(_ context.Context, add map[int64]int64) (map[int64]int64, error) {
	r.addCall++
	if r.addErr != nil {
		return nil, r.addErr
	}
	out := map[int64]int64{}
	for id, v := range add {
		r.users[id].UsedTraffic += v
		out[id] = r.users[id].UsedTraffic
	}
	return out, nil
}

type fakeState struct {
	rows    map[int64]State
	saveErr error
}

func (s *fakeState) Load(context.Context) (map[int64]State, error) {
	out := map[int64]State{}
	for k, v := range s.rows {
		out[k] = v
	}
	return out, nil
}

func (s *fakeState) Save(_ context.Context, states []State) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	for _, st := range states {
		s.rows[st.RebeccaID] = st
	}
	return nil
}

type env struct {
	panel   *fakePanel
	rebecca *fakeRebecca
	state   *fakeState
	engine  *Engine
}

func newEnv(t *testing.T, users ...RebeccaUser) *env {
	t.Helper()
	e := &env{panel: newFakePanel(), rebecca: &fakeRebecca{users: map[int64]*RebeccaUser{}}, state: &fakeState{rows: map[int64]State{}}}
	for i := range users {
		u := users[i]
		e.rebecca.users[u.ID] = &u
	}
	e.engine = &Engine{Panel: e.panel, Rebecca: e.rebecca, State: e.state,
		Opt: Options{DataLimit: 300 * gb, ResetAt: 250 * gb, Concurrency: 4, Logf: t.Logf,
			Now: func() time.Time { return time.Unix(1_800_000_000, 0) }}}
	return e
}

func (e *env) run(t *testing.T) Stats {
	t.Helper()
	stats, err := e.engine.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	return stats
}

const (
	uuidA = "11111111-2222-3333-4444-555555555555"
	uuidB = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
)

func alice() RebeccaUser {
	return RebeccaUser{ID: 1, Username: "alice", Status: "active", UsedTraffic: 5 * gb, DataLimit: 50 * gb, Expire: 1_900_000_000, UUID: uuidA}
}

// ---- tests -------------------------------------------------------------

func TestCreatesUserWithSameUUIDExpireAllGroupsAnd300GB(t *testing.T) {
	e := newEnv(t, alice())
	stats := e.run(t)
	if stats.Created != 1 {
		t.Fatalf("created = %d", stats.Created)
	}
	u := e.panel.byName("alice")
	if u == nil || u.VlessID != uuidA || u.Expire != 1_900_000_000 || u.DataLimit != 300*gb || !sameIDSet(u.GroupIDs, []int64{1, 2, 3}) || u.Status != "active" {
		t.Fatalf("panel user = %+v", u)
	}
	st := e.state.rows[1]
	if st.PanelID != u.ID || st.PanelBaseline != 0 || st.RebeccaUsedAfter != 5*gb {
		t.Fatalf("state = %+v", st)
	}
	// Second cycle with nothing changed does nothing.
	stats = e.run(t)
	if stats.Created+stats.Modified+stats.Resets+stats.UsageUsers != 0 || len(e.panel.creates) != 1 {
		t.Fatalf("idle cycle did work: %+v", stats)
	}
}

func TestAddsPanelUsageToRebeccaAndKeepsRebeccaOwnUsage(t *testing.T) {
	e := newEnv(t, alice())
	e.run(t)
	e.panel.use("alice", 2*gb)
	e.rebecca.users[1].UsedTraffic += 1 * gb // Rebecca's own nodes
	stats := e.run(t)
	if stats.UsageBytes != 2*gb || e.rebecca.users[1].UsedTraffic != 8*gb {
		t.Fatalf("usage stats=%+v rebecca used=%d", stats, e.rebecca.users[1].UsedTraffic)
	}
	e.panel.use("alice", 1*gb)
	e.run(t)
	if e.rebecca.users[1].UsedTraffic != 9*gb {
		t.Fatalf("second delta: rebecca used=%d, want 9GB", e.rebecca.users[1].UsedTraffic)
	}
	st := e.state.rows[1]
	if st.PanelBaseline != 3*gb || st.PendingUsage != 0 || st.RebeccaUsedAfter != 9*gb {
		t.Fatalf("state = %+v", st)
	}
}

func TestRebeccaResetResetsPanelUsage(t *testing.T) {
	e := newEnv(t, alice())
	e.run(t)
	e.panel.use("alice", 4*gb)
	e.run(t)                           // rebecca now 9GB
	e.rebecca.users[1].UsedTraffic = 0 // admin pressed reset in Rebecca
	e.panel.use("alice", 1*gb)         // traffic after the reset
	stats := e.run(t)
	if stats.Resets != 1 || e.panel.byName("alice").UsedTraffic != 0 {
		t.Fatalf("panel not reset: %+v", stats)
	}
	if e.rebecca.users[1].UsedTraffic != 1*gb {
		t.Fatalf("rebecca used after reset = %d, want 1GB", e.rebecca.users[1].UsedTraffic)
	}
	e.panel.use("alice", 2*gb)
	e.run(t)
	if e.rebecca.users[1].UsedTraffic != 3*gb {
		t.Fatalf("baseline after reset wrong: rebecca used = %d, want 3GB", e.rebecca.users[1].UsedTraffic)
	}
}

func TestPanelUsageResetAtThresholdWithoutLosingUsage(t *testing.T) {
	big := alice()
	big.DataLimit = 0
	e := newEnv(t, big)
	e.run(t)
	e.panel.use("alice", 260*gb)
	stats := e.run(t)
	if stats.Resets != 1 || e.panel.byName("alice").UsedTraffic != 0 {
		t.Fatalf("expected threshold reset: %+v", stats)
	}
	if e.rebecca.users[1].UsedTraffic != 265*gb {
		t.Fatalf("usage lost on reset: %d", e.rebecca.users[1].UsedTraffic)
	}
	e.panel.use("alice", 3*gb)
	e.run(t)
	if e.rebecca.users[1].UsedTraffic != 268*gb {
		t.Fatalf("after reset: %d", e.rebecca.users[1].UsedTraffic)
	}
}

func TestPanelLimitedStatusIsResetEvenBelowThreshold(t *testing.T) {
	e := newEnv(t, alice())
	e.run(t)
	u := e.panel.byName("alice")
	u.UsedTraffic = 10 * gb
	u.Status = "limited"
	e.run(t)
	if e.panel.byName("alice").Status != "active" || e.panel.byName("alice").UsedTraffic != 0 {
		t.Fatalf("limited panel user not revived: %+v", *e.panel.byName("alice"))
	}
}

func TestRebeccaLimitedOrExpiredDisablesAndReEnables(t *testing.T) {
	e := newEnv(t, alice())
	e.run(t)
	for _, status := range []string{"limited", "expired", "disabled", "deleted"} {
		e.rebecca.users[1].Status = status
		e.run(t)
		if got := e.panel.byName("alice").Status; got != "disabled" {
			t.Fatalf("rebecca %s -> panel %s", status, got)
		}
		e.rebecca.users[1].Status = "active"
		e.run(t)
		if got := e.panel.byName("alice").Status; got != "active" {
			t.Fatalf("rebecca active again -> panel %s", got)
		}
	}
}

func TestRebeccaEditOfExpireAndUUIDIsMirrored(t *testing.T) {
	e := newEnv(t, alice())
	e.run(t)
	e.rebecca.users[1].Expire = 1_950_000_000
	e.rebecca.users[1].UUID = uuidB
	stats := e.run(t)
	u := e.panel.byName("alice")
	if stats.Modified != 1 || u.Expire != 1_950_000_000 || u.VlessID != uuidB {
		t.Fatalf("edit not mirrored: stats=%+v user=%+v", stats, *u)
	}
	e.rebecca.users[1].Expire = 0 // made unlimited
	e.run(t)
	if e.panel.byName("alice").Expire != 0 {
		t.Fatalf("unlimited expire not mirrored")
	}
}

func TestUsernameFallbackWrapsWithA(t *testing.T) {
	odd := alice()
	odd.Username = "x"
	e := newEnv(t, odd)
	e.panel.rejectRaw["x"] = true
	e.run(t)
	if e.panel.byName("axa") == nil {
		t.Fatalf("fallback username not used: creates=%+v", e.panel.creates)
	}
	if e.state.rows[1].PanelUsername != "axa" {
		t.Fatalf("state username = %q", e.state.rows[1].PanelUsername)
	}
}

func TestUsernameTakenByOtherUUIDUsesFallback(t *testing.T) {
	e := newEnv(t, alice())
	e.panel.users[5] = &PanelUser{ID: 5, Username: "alice", Status: "active", VlessID: uuidB}
	e.run(t)
	u := e.panel.byName("aalicea")
	if u == nil || u.VlessID != uuidA {
		t.Fatalf("expected aalicea with alice's uuid, got %+v", u)
	}
	if e.panel.users[5].VlessID != uuidB {
		t.Fatalf("foreign user was modified")
	}
}

func TestAdoptsExistingPanelUserWithSameUUIDWithoutDoubleCounting(t *testing.T) {
	e := newEnv(t, alice())
	e.panel.users[7] = &PanelUser{ID: 7, Username: "alice", Status: "active", UsedTraffic: 40 * gb,
		DataLimit: 300 * gb, Expire: 1_900_000_000, VlessID: uuidA, GroupIDs: []int64{1, 2, 3}}
	stats := e.run(t)
	if stats.Created != 0 || stats.Adopted != 1 || e.rebecca.users[1].UsedTraffic != 5*gb {
		t.Fatalf("adopt: stats=%+v used=%d", stats, e.rebecca.users[1].UsedTraffic)
	}
	e.panel.use("alice", 1*gb)
	e.run(t)
	if e.rebecca.users[1].UsedTraffic != 6*gb {
		t.Fatalf("after adopt only new usage should count, used=%d", e.rebecca.users[1].UsedTraffic)
	}
}

func TestPendingUsageSurvivesRebeccaWriteFailure(t *testing.T) {
	e := newEnv(t, alice())
	e.run(t)
	e.panel.use("alice", 2*gb)
	e.rebecca.addErr = errors.New("db down")
	if _, err := e.engine.RunOnce(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if e.state.rows[1].PendingUsage != 2*gb {
		t.Fatalf("pending not kept: %+v", e.state.rows[1])
	}
	e.rebecca.addErr = nil
	e.run(t)
	if e.rebecca.users[1].UsedTraffic != 7*gb || e.state.rows[1].PendingUsage != 0 {
		t.Fatalf("pending not flushed exactly once: used=%d state=%+v", e.rebecca.users[1].UsedTraffic, e.state.rows[1])
	}
}

func TestDeletedRebeccaRowDisablesPanelUser(t *testing.T) {
	e := newEnv(t, alice())
	e.run(t)
	delete(e.rebecca.users, 1)
	e.run(t)
	if e.panel.byName("alice").Status != "disabled" {
		t.Fatalf("orphan not disabled")
	}
}

func TestInactiveRebeccaUserIsNotCreatedUntilActive(t *testing.T) {
	for _, status := range []string{"limited", "expired", "disabled", "deleted"} {
		u := alice()
		u.Status = status
		e := newEnv(t, u)
		stats := e.run(t)
		if stats.Created != 0 || len(e.panel.creates) != 0 || len(e.state.rows) != 0 {
			t.Fatalf("%s user was created: stats=%+v", status, stats)
		}
		if status == "deleted" {
			continue
		}
		e.rebecca.users[1].Status = "active" // renewed / reset / enabled in Rebecca
		stats = e.run(t)
		got := e.panel.byName("alice")
		if stats.Created != 1 || got == nil || got.Status != "active" || got.VlessID != uuidA {
			t.Fatalf("%s -> active: not created: stats=%+v user=%+v", status, stats, got)
		}
	}
}

func TestOnHoldUserHasNoExpireAndIsActive(t *testing.T) {
	u := alice()
	u.Status = "on_hold"
	u.Expire = 0
	e := newEnv(t, u)
	e.run(t)
	if got := e.panel.byName("alice"); got.Status != "active" || got.Expire != 0 {
		t.Fatalf("got %+v", *got)
	}
}

func TestUsersWithoutUUIDAreSkipped(t *testing.T) {
	u := alice()
	u.UUID = ""
	e := newEnv(t, u)
	stats := e.run(t)
	if stats.Skipped != 1 || len(e.panel.creates) != 0 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestDryRunChangesNothing(t *testing.T) {
	e := newEnv(t, alice())
	e.engine.Opt.DryRun = true
	stats := e.run(t)
	if stats.Created != 1 || len(e.panel.users) != 0 || len(e.state.rows) != 0 {
		t.Fatalf("dry run wrote: stats=%+v", stats)
	}
}

func TestNoGroupsRefusesToCreate(t *testing.T) {
	e := newEnv(t, alice())
	e.panel.groups = nil
	if _, err := e.engine.RunOnce(context.Background()); err == nil {
		t.Fatal("expected error without groups")
	}
}

func TestManyUsers(t *testing.T) {
	var users []RebeccaUser
	for i := int64(1); i <= 1000; i++ {
		u := alice()
		u.ID = i
		u.Username = "user" + string(rune('a'+i%26)) + strings.Repeat("x", int(i%5)) + itoa(i)
		u.UUID = "00000000-0000-0000-0000-" + pad12(i)
		users = append(users, u)
	}
	e := newEnv(t, users...)
	stats := e.run(t)
	if stats.Created != 1000 || len(e.state.rows) != 1000 {
		t.Fatalf("stats=%+v", stats)
	}
	for _, u := range e.panel.users {
		u.UsedTraffic = gb
	}
	stats = e.run(t)
	if stats.UsageUsers != 1000 || stats.UsageBytes != 1000*gb {
		t.Fatalf("stats=%+v", stats)
	}
}

func itoa(i int64) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	s := ""
	for i > 0 {
		s = string(digits[i%10]) + s
		i /= 10
	}
	return s
}

func pad12(i int64) string {
	s := itoa(i)
	return strings.Repeat("0", 12-len(s)) + s
}
