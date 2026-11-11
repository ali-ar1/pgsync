package syncer

import (
	"encoding/hex"
	"encoding/json"
	"strings"
)

// DeriveUUID returns the VLESS UUID Rebecca gives a user: the id stored in the
// user's vless credential row, otherwise the credential_key written as a UUID.
// This mirrors Rebecca's own rule (user_credentials settings id, then key).
func DeriveUUID(vlessSettingsJSON string, credentialKey string) string {
	if strings.TrimSpace(vlessSettingsJSON) != "" {
		var settings map[string]any
		if json.Unmarshal([]byte(vlessSettingsJSON), &settings) == nil {
			for _, key := range []string{"id", "uuid"} {
				if value, ok := settings[key].(string); ok {
					if id, ok := normalizeUUID(value); ok {
						return id
					}
				}
			}
		}
	}
	if id, ok := normalizeUUID(credentialKey); ok {
		return id
	}
	return ""
}

func normalizeUUID(value string) (string, bool) {
	cleaned := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "-", ""))
	if len(cleaned) != 32 {
		return "", false
	}
	if _, err := hex.DecodeString(cleaned); err != nil {
		return "", false
	}
	return cleaned[:8] + "-" + cleaned[8:12] + "-" + cleaned[12:16] + "-" + cleaned[16:20] + "-" + cleaned[20:], true
}

func sameUUID(a, b string) bool {
	na, okA := normalizeUUID(a)
	nb, okB := normalizeUUID(b)
	return okA && okB && na == nb
}
