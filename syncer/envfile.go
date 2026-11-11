package syncer

import (
	"bufio"
	"net/url"
	"os"
	"sort"
	"strings"
)

// ParseEnv reads KEY=value lines (values may be "double" or 'single' quoted).
func ParseEnv(text string) map[string]string {
	out := map[string]string{}
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		eq := strings.Index(line, "=")
		if eq <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:eq])
		value := strings.TrimSpace(line[eq+1:])
		switch {
		case len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"':
			value = unescapeDouble(value[1 : len(value)-1])
		case len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'':
			value = value[1 : len(value)-1]
		default:
			if hash := strings.Index(value, " #"); hash >= 0 {
				value = strings.TrimSpace(value[:hash])
			}
		}
		out[key] = value
	}
	return out
}

func unescapeDouble(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			switch s[i] {
			case 'n':
				b.WriteByte('\n')
			default:
				b.WriteByte(s[i])
			}
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// FormatEnv writes values double-quoted with escaping, keys in the given
// order first and any others sorted after.
func FormatEnv(values map[string]string, order []string) string {
	var b strings.Builder
	b.WriteString("# pgsync settings (Rebecca -> PasarGuard). Keep this file private.\n")
	written := map[string]bool{}
	write := func(key string) {
		value, ok := values[key]
		if !ok || written[key] {
			return
		}
		written[key] = true
		escaped := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(value)
		b.WriteString(key + `="` + escaped + "\"\n")
	}
	for _, key := range order {
		write(key)
	}
	var rest []string
	for key := range values {
		if !written[key] {
			rest = append(rest, key)
		}
	}
	sort.Strings(rest)
	for _, key := range rest {
		write(key)
	}
	return b.String()
}

// ReadEnvFile returns the parsed file, or an empty map if it does not exist.
func ReadEnvFile(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	return ParseEnv(string(data)), nil
}

// DatabaseURL is a parsed SQLAlchemy-style MySQL URL.
type DatabaseURL struct {
	User, Password, Host, Port, Name string
}

// ParseDatabaseURL understands Rebecca's SQLALCHEMY_DATABASE_URL, e.g.
// mysql+pymysql://rebecca:p%40ss@127.0.0.1:3306/rebecca?charset=utf8mb4
func ParseDatabaseURL(raw string) (DatabaseURL, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "mysql") && !strings.HasPrefix(raw, "mariadb") {
		return DatabaseURL{}, false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return DatabaseURL{}, false
	}
	out := DatabaseURL{Host: u.Hostname(), Port: u.Port(), Name: strings.TrimPrefix(u.Path, "/")}
	if u.User != nil {
		out.User = u.User.Username()
		out.Password, _ = u.User.Password()
	}
	if out.Port == "" {
		out.Port = "3306"
	}
	return out, true
}
