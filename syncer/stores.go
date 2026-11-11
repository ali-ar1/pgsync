package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const sqlChunk = 500

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func chunkIDs(ids []int64, size int, fn func([]int64) error) error {
	for start := 0; start < len(ids); start += size {
		end := start + size
		if end > len(ids) {
			end = len(ids)
		}
		if err := fn(ids[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// RebeccaDB reads users from and writes usage into Rebecca's MySQL database.
type RebeccaDB struct{ DB *sql.DB }

// ListUsers returns every user with the UUID Rebecca would give them.
func (r RebeccaDB) ListUsers(ctx context.Context) ([]RebeccaUser, error) {
	rows, err := r.DB.QueryContext(ctx, `
SELECT u.id,
       COALESCE(u.username, ''),
       LOWER(COALESCE(u.status, '')),
       COALESCE(u.used_traffic, 0),
       COALESCE(u.data_limit, 0),
       COALESCE(u.expire, 0),
       COALESCE(u.credential_key, ''),
       COALESCE((SELECT CAST(c.settings AS CHAR)
                   FROM user_credentials c
                  WHERE c.user_id = u.id AND LOWER(c.type) = 'vless'
                  ORDER BY c.id LIMIT 1), '')
  FROM users u
 ORDER BY u.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []RebeccaUser
	for rows.Next() {
		var u RebeccaUser
		var credentialKey, vlessSettings string
		if err := rows.Scan(&u.ID, &u.Username, &u.Status, &u.UsedTraffic, &u.DataLimit, &u.Expire, &credentialKey, &vlessSettings); err != nil {
			return nil, err
		}
		u.UUID = DeriveUUID(vlessSettings, credentialKey)
		users = append(users, u)
	}
	return users, rows.Err()
}

// AddUsage adds bytes to used_traffic in one transaction, the same atomic
// "used_traffic = used_traffic + x" pattern Rebecca itself uses.
func (r RebeccaDB) AddUsage(ctx context.Context, add map[int64]int64) (map[int64]int64, error) {
	ids := make([]int64, 0, len(add))
	for id, value := range add {
		if value > 0 {
			ids = append(ids, id)
		}
	}
	result := make(map[int64]int64, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	err = chunkIDs(ids, sqlChunk, func(chunk []int64) error {
		var b strings.Builder
		b.WriteString("UPDATE users SET used_traffic = COALESCE(used_traffic, 0) + CASE id ")
		args := make([]any, 0, len(chunk)*3)
		for _, id := range chunk {
			b.WriteString("WHEN ? THEN ? ")
			args = append(args, id, add[id])
		}
		b.WriteString("ELSE 0 END WHERE id IN (" + placeholders(len(chunk)) + ")")
		for _, id := range chunk {
			args = append(args, id)
		}
		if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
			return err
		}
		inArgs := make([]any, len(chunk))
		for i, id := range chunk {
			inArgs[i] = id
		}
		rows, err := tx.QueryContext(ctx, "SELECT id, COALESCE(used_traffic, 0) FROM users WHERE id IN ("+placeholders(len(chunk))+")", inArgs...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, used int64
			if err := rows.Scan(&id, &used); err != nil {
				return err
			}
			result[id] = used
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return result, tx.Commit()
}

// StateDB keeps the script's bookkeeping in its own MySQL database.
type StateDB struct{ DB *sql.DB }

// Migrate creates the state table if needed.
func (s StateDB) Migrate(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS pgsync_users (
  rebecca_id BIGINT NOT NULL PRIMARY KEY,
  rebecca_username VARCHAR(128) NOT NULL DEFAULT '',
  uuid VARCHAR(36) NOT NULL DEFAULT '',
  panel_id BIGINT NOT NULL DEFAULT 0,
  panel_username VARCHAR(128) NOT NULL DEFAULT '',
  panel_baseline BIGINT NOT NULL DEFAULT 0,
  rebecca_used_after BIGINT NOT NULL DEFAULT 0,
  pending_usage BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	return err
}

// Load reads every state row.
func (s StateDB) Load(ctx context.Context) (map[int64]State, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT rebecca_id, rebecca_username, uuid, panel_id, panel_username,
       panel_baseline, rebecca_used_after, pending_usage FROM pgsync_users`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]State{}
	for rows.Next() {
		var st State
		if err := rows.Scan(&st.RebeccaID, &st.RebeccaUsername, &st.UUID, &st.PanelID, &st.PanelUsername,
			&st.PanelBaseline, &st.RebeccaUsedAfter, &st.PendingUsage); err != nil {
			return nil, err
		}
		out[st.RebeccaID] = st
	}
	return out, rows.Err()
}

// Save upserts state rows in one transaction.
func (s StateDB) Save(ctx context.Context, states []State) error {
	if len(states) == 0 {
		return nil
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	for start := 0; start < len(states); start += sqlChunk {
		end := start + sqlChunk
		if end > len(states) {
			end = len(states)
		}
		chunk := states[start:end]
		var b strings.Builder
		b.WriteString(`INSERT INTO pgsync_users (rebecca_id, rebecca_username, uuid, panel_id, panel_username,
  panel_baseline, rebecca_used_after, pending_usage) VALUES `)
		args := make([]any, 0, len(chunk)*8)
		for i, st := range chunk {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString("(?,?,?,?,?,?,?,?)")
			args = append(args, st.RebeccaID, st.RebeccaUsername, st.UUID, st.PanelID, st.PanelUsername,
				st.PanelBaseline, st.RebeccaUsedAfter, st.PendingUsage)
		}
		b.WriteString(` ON DUPLICATE KEY UPDATE rebecca_username = VALUES(rebecca_username), uuid = VALUES(uuid),
  panel_id = VALUES(panel_id), panel_username = VALUES(panel_username), panel_baseline = VALUES(panel_baseline),
  rebecca_used_after = VALUES(rebecca_used_after), pending_usage = VALUES(pending_usage)`)
		if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
	}
	return tx.Commit()
}
