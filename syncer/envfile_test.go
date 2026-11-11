package syncer

import "testing"

func TestEnvRoundTrip(t *testing.T) {
	values := map[string]string{
		"PG_PASS":   `Net@09"37\Net #x`,
		"PG_URL":    "https://panel.example.com",
		"ZZ_EXTRA":  "1",
		"STATE_DB":  "",
		"MULTILINE": "a\nb",
	}
	text := FormatEnv(values, []string{"PG_URL", "PG_PASS"})
	got := ParseEnv(text)
	for k, v := range values {
		if got[k] != v {
			t.Fatalf("%s: got %q want %q\n%s", k, got[k], v, text)
		}
	}
}

func TestParseEnvRebeccaStyle(t *testing.T) {
	got := ParseEnv(`
# comment
SQLALCHEMY_DATABASE_URL = "mysql+pymysql://rebecca:p%40ss%3Aw@127.0.0.1:3306/rebecca"
UVICORN_PORT = 8000 # inline
SINGLE='x y'
export E=1
`)
	if got["UVICORN_PORT"] != "8000" || got["SINGLE"] != "x y" || got["E"] != "1" {
		t.Fatalf("parsed %v", got)
	}
	db, ok := ParseDatabaseURL(got["SQLALCHEMY_DATABASE_URL"])
	if !ok || db.User != "rebecca" || db.Password != "p@ss:w" || db.Host != "127.0.0.1" || db.Port != "3306" || db.Name != "rebecca" {
		t.Fatalf("db url %+v %v", db, ok)
	}
	if _, ok := ParseDatabaseURL("sqlite:////var/lib/rebecca/db.sqlite3"); ok {
		t.Fatal("sqlite must not parse as mysql")
	}
}
