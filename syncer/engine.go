package syncer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Options tunes the engine.
type Options struct {
	DataLimit   int64 // bytes given to every PasarGuard user (e.g. 300 GB)
	ResetAt     int64 // reset PasarGuard usage once it reaches this many bytes
	Concurrency int   // parallel PasarGuard calls
	DryRun      bool  // plan only: no API writes, no DB writes
	Logf        func(format string, args ...any)
	Now         func() time.Time
}

// Engine runs sync cycles.
type Engine struct {
	Panel   Panel
	Rebecca Rebecca
	State   StateStore
	Opt     Options

	mu        sync.Mutex
	failUntil map[int64]time.Time // creation back-off per Rebecca user
}

const createRetryAfter = 30 * time.Minute

func (e *Engine) logf(format string, args ...any) {
	if e.Opt.Logf != nil {
		e.Opt.Logf(format, args...)
	}
}

func (e *Engine) now() time.Time {
	if e.Opt.Now != nil {
		return e.Opt.Now()
	}
	return time.Now()
}

// PanelUsernames returns the names tried for a Rebecca user, in order:
// the Rebecca username, then the same name wrapped in "a...a".
func PanelUsernames(rebeccaUsername string) []string {
	name := strings.TrimSpace(rebeccaUsername)
	return []string{name, "a" + name + "a"}
}

type task struct {
	rb       RebeccaUser
	st       State
	hasState bool
	panel    *PanelUser
	orphan   bool

	create  bool
	desired UserInput
	modify  bool
	reset   bool
	delta   int64
	panelAt int64 // PasarGuard used_traffic observed this cycle

	// results
	created  bool
	adopted  bool
	modified bool
	didReset bool
	failed   bool
}

func (e *Engine) desiredFor(rb RebeccaUser, groups []int64) UserInput {
	status := "disabled"
	if rb.Active() {
		status = "active"
	}
	expire := rb.Expire
	if strings.EqualFold(rb.Status, "on_hold") || expire < 0 {
		expire = 0
	}
	return UserInput{
		Username:  rb.Username,
		UUID:      rb.UUID,
		Status:    status,
		DataLimit: e.Opt.DataLimit,
		Expire:    expire,
		GroupIDs:  groups,
	}
}

func sameIDSet(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]int64(nil), a...)
	y := append([]int64(nil), b...)
	sort.Slice(x, func(i, j int) bool { return x[i] < x[j] })
	sort.Slice(y, func(i, j int) bool { return y[i] < y[j] })
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// needsModify compares PasarGuard's current user with what Rebecca wants.
func needsModify(pg PanelUser, want UserInput) bool {
	switch {
	case want.Status == "disabled" && pg.Status != "disabled":
		return true
	case want.Status == "active" && (pg.Status == "disabled" || pg.Status == "expired"):
		return true
	}
	diff := pg.Expire - want.Expire
	if diff < -60 || diff > 60 {
		return true
	}
	if pg.DataLimit != want.DataLimit {
		return true
	}
	if pg.GroupIDs != nil && !sameIDSet(pg.GroupIDs, want.GroupIDs) {
		return true
	}
	if pg.VlessID != "" && !sameUUID(pg.VlessID, want.UUID) {
		return true
	}
	return false
}

// RunOnce performs one full sync cycle.
func (e *Engine) RunOnce(ctx context.Context) (Stats, error) {
	started := e.now()
	stats := Stats{}

	states, err := e.State.Load(ctx)
	if err != nil {
		return stats, fmt.Errorf("load state: %w", err)
	}
	if err := e.flushPending(ctx, states); err != nil {
		return stats, err
	}

	rbUsers, err := e.Rebecca.ListUsers(ctx)
	if err != nil {
		return stats, fmt.Errorf("read rebecca users: %w", err)
	}
	panelUsers, err := e.Panel.ListUsers(ctx)
	if err != nil {
		return stats, fmt.Errorf("read pasarguard users: %w", err)
	}
	groups, err := e.Panel.GroupIDs(ctx)
	if err != nil {
		return stats, fmt.Errorf("read pasarguard groups: %w", err)
	}
	if len(groups) == 0 {
		return stats, errors.New("pasarguard has no groups; refusing to create users without inbounds")
	}
	stats.RebeccaUsers = len(rbUsers)
	stats.PanelUsers = len(panelUsers)

	byID := make(map[int64]*PanelUser, len(panelUsers))
	byName := make(map[string]*PanelUser, len(panelUsers))
	for i := range panelUsers {
		pu := &panelUsers[i]
		byID[pu.ID] = pu
		byName[strings.ToLower(pu.Username)] = pu
	}

	now := e.now()
	seen := make(map[int64]bool, len(rbUsers))
	tasks := make([]*task, 0, len(rbUsers))

	for _, rb := range rbUsers {
		if rb.UUID == "" || strings.TrimSpace(rb.Username) == "" {
			stats.Skipped++
			continue
		}
		seen[rb.ID] = true
		t := &task{rb: rb}
		t.st, t.hasState = states[rb.ID]
		if t.hasState && t.st.PanelID > 0 {
			t.panel = byID[t.st.PanelID]
		}
		t.desired = e.desiredFor(rb, groups)

		if t.panel == nil {
			// Adopt an existing PasarGuard user with the same UUID (state lost or
			// user created earlier) instead of creating a duplicate.
			for _, name := range PanelUsernames(rb.Username) {
				if candidate := byName[strings.ToLower(name)]; candidate != nil && sameUUID(candidate.VlessID, rb.UUID) {
					t.panel = candidate
					t.adopted = true
					t.hasState = true
					t.st = State{RebeccaID: rb.ID, PanelID: candidate.ID, PanelUsername: candidate.Username,
						PanelBaseline: candidate.UsedTraffic, RebeccaUsedAfter: rb.UsedTraffic}
					break
				}
			}
		}

		if t.panel == nil {
			if strings.EqualFold(rb.Status, "deleted") {
				continue
			}
			e.mu.Lock()
			blockedUntil := e.failUntil[rb.ID]
			e.mu.Unlock()
			if now.Before(blockedUntil) {
				stats.Skipped++
				continue
			}
			t.create = true
			tasks = append(tasks, t)
			continue
		}

		pg := *t.panel
		t.panelAt = pg.UsedTraffic
		if t.hasState && !t.adopted {
			delta := pg.UsedTraffic - t.st.PanelBaseline
			if delta < 0 { // PasarGuard usage was reset outside this cycle
				delta = pg.UsedTraffic
			}
			t.delta = delta
			if rb.UsedTraffic < t.st.RebeccaUsedAfter { // Rebecca reset/lowered usage
				t.reset = pg.UsedTraffic > 0
			}
		}
		if pg.UsedTraffic > 0 && ((e.Opt.ResetAt > 0 && pg.UsedTraffic >= e.Opt.ResetAt) || pg.Status == "limited") {
			t.reset = true
		}
		t.modify = needsModify(pg, t.desired)
		tasks = append(tasks, t)
	}

	// Rebecca rows that disappeared: disable their PasarGuard user.
	for id, st := range states {
		if seen[id] || st.PanelID <= 0 {
			continue
		}
		pg := byID[st.PanelID]
		if pg == nil || pg.Status == "disabled" {
			continue
		}
		tasks = append(tasks, &task{st: st, hasState: true, panel: pg, orphan: true, modify: true,
			desired: UserInput{Status: "disabled", DataLimit: -1, Expire: -1}})
	}

	e.execute(ctx, tasks)

	var save []State
	for _, t := range tasks {
		switch {
		case t.failed:
			stats.Errors++
		}
		if t.created {
			stats.Created++
		}
		if t.adopted {
			stats.Adopted++
		}
		if t.modified {
			stats.Modified++
			if t.desired.Status == "disabled" {
				stats.Disabled++
			}
		}
		if t.didReset {
			stats.Resets++
		}
		if !t.hasState || t.panel == nil {
			continue
		}
		st := t.st
		if !t.orphan {
			st.RebeccaID = t.rb.ID
			st.RebeccaUsername = t.rb.Username
			st.UUID = t.rb.UUID
			st.RebeccaUsedAfter = t.rb.UsedTraffic
		}
		st.PanelID = t.panel.ID
		st.PanelUsername = t.panel.Username
		if !t.created && !t.adopted {
			st.PanelBaseline = t.panelAt
			if t.didReset {
				st.PanelBaseline = 0
			}
			if t.delta > 0 {
				st.PendingUsage += t.delta
				stats.UsageUsers++
				stats.UsageBytes += t.delta
			}
		}
		save = append(save, st)
	}

	if e.Opt.DryRun {
		stats.Duration = e.now().Sub(started)
		return stats, nil
	}
	// Persist the new baselines (with the usage marked pending) before touching
	// Rebecca, so a crash can never count the same PasarGuard traffic twice.
	if err := e.State.Save(ctx, save); err != nil {
		return stats, fmt.Errorf("save state: %w", err)
	}
	pending := map[int64]State{}
	for _, st := range save {
		if st.PendingUsage > 0 {
			pending[st.RebeccaID] = st
		}
	}
	if err := e.flushPending(ctx, pending); err != nil {
		return stats, err
	}
	stats.Duration = e.now().Sub(started)
	return stats, nil
}

// flushPending writes pending usage into Rebecca and clears it.
func (e *Engine) flushPending(ctx context.Context, states map[int64]State) error {
	add := map[int64]int64{}
	for id, st := range states {
		if st.PendingUsage > 0 {
			add[id] = st.PendingUsage
		}
	}
	if len(add) == 0 || e.Opt.DryRun {
		return nil
	}
	newUsed, err := e.Rebecca.AddUsage(ctx, add)
	if err != nil {
		return fmt.Errorf("add usage to rebecca: %w", err)
	}
	cleared := make([]State, 0, len(add))
	for id := range add {
		st := states[id]
		st.PendingUsage = 0
		if used, ok := newUsed[id]; ok {
			st.RebeccaUsedAfter = used
		}
		states[id] = st
		cleared = append(cleared, st)
	}
	if err := e.State.Save(ctx, cleared); err != nil {
		return fmt.Errorf("clear pending usage: %w", err)
	}
	return nil
}

func (e *Engine) execute(ctx context.Context, tasks []*task) {
	workers := e.Opt.Concurrency
	if workers <= 0 {
		workers = 8
	}
	queue := make(chan *task)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range queue {
				e.runTask(ctx, t)
			}
		}()
	}
	for _, t := range tasks {
		if t.create || t.modify || t.reset {
			queue <- t
		}
	}
	close(queue)
	wg.Wait()
}

func retryableName(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.Status {
	case http.StatusBadRequest, http.StatusConflict, http.StatusUnprocessableEntity:
		return true
	}
	return false
}

func (e *Engine) runTask(ctx context.Context, t *task) {
	if e.Opt.DryRun {
		if t.create {
			t.created = true
			t.hasState = true
			t.panel = &PanelUser{Username: t.rb.Username}
		}
		t.didReset = t.reset
		t.modified = t.modify
		return
	}
	if t.create {
		var lastErr error
		for _, name := range PanelUsernames(t.rb.Username) {
			in := t.desired
			in.Username = name
			created, err := e.Panel.CreateUser(ctx, in)
			if err == nil {
				t.panel = &created
				t.created = true
				t.hasState = true
				t.st = State{RebeccaID: t.rb.ID, PanelID: created.ID, PanelUsername: created.Username,
					PanelBaseline: 0, RebeccaUsedAfter: t.rb.UsedTraffic}
				lastErr = nil
				break
			}
			lastErr = err
			if !retryableName(err) {
				break
			}
		}
		if lastErr != nil {
			t.failed = true
			e.mu.Lock()
			if e.failUntil == nil {
				e.failUntil = map[int64]time.Time{}
			}
			e.failUntil[t.rb.ID] = e.now().Add(createRetryAfter)
			e.mu.Unlock()
			e.logf("create %q failed (retry in %s): %v", t.rb.Username, createRetryAfter, lastErr)
			return
		}
		// Created users start active; mirror a disabled Rebecca status right away.
		t.modify = t.desired.Status == "disabled"
	}
	if t.reset && t.panel != nil {
		if err := e.Panel.ResetUsage(ctx, *t.panel); err != nil {
			t.failed = true
			e.logf("reset usage of %q failed: %v", t.panel.Username, err)
		} else {
			t.didReset = true
		}
	}
	if t.modify && t.panel != nil {
		if err := e.Panel.ModifyUser(ctx, *t.panel, t.desired); err != nil {
			t.failed = true
			e.logf("update %q failed: %v", t.panel.Username, err)
		} else {
			t.modified = true
		}
	}
}
