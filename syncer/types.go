// Package syncer mirrors Rebecca users into a PasarGuard panel and folds the
// traffic PasarGuard records back into Rebecca's used_traffic.
package syncer

import (
	"context"
	"strings"
	"time"
)

// RebeccaUser is one row of Rebecca's users table, reduced to what the sync needs.
type RebeccaUser struct {
	ID          int64
	Username    string
	Status      string // active, on_hold, disabled, limited, expired, deleted
	UsedTraffic int64
	DataLimit   int64 // 0 = unlimited
	Expire      int64 // unix seconds, 0 = never
	UUID        string
}

// Active reports whether Rebecca lets this user connect.
func (u RebeccaUser) Active() bool {
	switch strings.ToLower(u.Status) {
	case "active", "on_hold":
		return true
	}
	return false
}

// PanelUser is a PasarGuard user as returned by its API.
type PanelUser struct {
	ID          int64
	Username    string
	Status      string
	UsedTraffic int64
	DataLimit   int64
	Expire      int64   // unix seconds, 0 = never
	VlessID     string  // empty when the API did not include proxy settings
	GroupIDs    []int64 // nil when the API did not include groups
}

// UserInput is what we send to PasarGuard on create/modify.
// For modify, DataLimit < 0 or Expire < 0 means "leave unchanged",
// a nil GroupIDs means "leave groups", and an empty UUID leaves proxy settings.
type UserInput struct {
	Username  string
	UUID      string
	Status    string // active | disabled
	DataLimit int64
	Expire    int64
	GroupIDs  []int64
}

// State is the script's own bookkeeping for one Rebecca user.
type State struct {
	RebeccaID        int64
	RebeccaUsername  string
	UUID             string
	PanelID          int64
	PanelUsername    string
	PanelBaseline    int64 // PasarGuard used_traffic already counted into Rebecca
	RebeccaUsedAfter int64 // Rebecca used_traffic right after our last write (reset detection)
	PendingUsage     int64 // counted from PasarGuard but not yet written to Rebecca
}

// Panel is the PasarGuard API surface the engine needs.
type Panel interface {
	ListUsers(ctx context.Context) ([]PanelUser, error)
	GroupIDs(ctx context.Context) ([]int64, error)
	CreateUser(ctx context.Context, in UserInput) (PanelUser, error)
	ModifyUser(ctx context.Context, user PanelUser, in UserInput) error
	ResetUsage(ctx context.Context, user PanelUser) error
}

// Rebecca is the Rebecca database surface the engine needs.
type Rebecca interface {
	ListUsers(ctx context.Context) ([]RebeccaUser, error)
	// AddUsage atomically adds bytes to used_traffic and returns the new values.
	AddUsage(ctx context.Context, add map[int64]int64) (map[int64]int64, error)
}

// StateStore persists State rows in the script's own database.
type StateStore interface {
	Load(ctx context.Context) (map[int64]State, error)
	Save(ctx context.Context, states []State) error
}

// Stats summarises one sync cycle.
type Stats struct {
	RebeccaUsers int
	PanelUsers   int
	Created      int
	Adopted      int
	Modified     int
	Disabled     int
	Resets       int
	UsageUsers   int
	UsageBytes   int64
	Skipped      int
	Errors       int
	Duration     time.Duration
}
