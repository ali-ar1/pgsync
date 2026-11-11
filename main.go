// Command pgsync mirrors Rebecca users into a PasarGuard panel (same UUID,
// expire and status) and adds the traffic PasarGuard records to Rebecca.
//
//	pgsync            first run: interactive setup, then sync every minute
//	pgsync setup      (re)enter credentials and test every connection
//	pgsync run        sync forever (used by the systemd service)
//	pgsync once       one sync cycle
//	pgsync dry-run    one cycle that only reports what it would do
package main

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/ali-ar1/pgsync/syncer"
)

const (
	keyStateHost   = "STATE_DB_HOST"
	keyStatePort   = "STATE_DB_PORT"
	keyStateName   = "STATE_DB_NAME"
	keyStateUser   = "STATE_DB_USER"
	keyStatePass   = "STATE_DB_PASS"
	keyPanelURL    = "PG_URL"
	keyPanelUser   = "PG_USER"
	keyPanelPass   = "PG_PASS"
	keyRebHost     = "REBECCA_DB_HOST"
	keyRebPort     = "REBECCA_DB_PORT"
	keyRebName     = "REBECCA_DB_NAME"
	keyRebUser     = "REBECCA_DB_USER"
	keyRebPass     = "REBECCA_DB_PASS"
	keyInterval    = "SYNC_INTERVAL_SECONDS"
	keyLimitGB     = "PG_DATA_LIMIT_GB"
	keyResetGB     = "PG_RESET_AT_GB"
	keyConcurrency = "CONCURRENCY"

	defaultPanelURL   = "https://osirus.videobazi.com"
	rebeccaEnvFile    = "/opt/rebecca/.env"
	defaultInterval   = "60"
	defaultLimitGB    = "300"
	defaultResetGB    = "250"
	defaultConcurrent = "8"
)

var envOrder = []string{
	keyStateHost, keyStatePort, keyStateName, keyStateUser, keyStatePass,
	keyPanelURL, keyPanelUser, keyPanelPass,
	keyRebHost, keyRebPort, keyRebName, keyRebUser, keyRebPass,
	keyInterval, keyLimitGB, keyResetGB, keyConcurrency,
}

func defaultEnvPath() string {
	if exe, err := os.Executable(); err == nil {
		return filepath.Join(filepath.Dir(exe), ".env")
	}
	return ".env"
}

func main() {
	log.SetFlags(log.LstdFlags)
	envPath := flag.String("env", defaultEnvPath(), "path of the settings file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: pgsync [-env path] [setup|run|once|dry-run]\n")
	}
	flag.Parse()
	command := flag.Arg(0)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch command {
	case "":
		if _, statErr := os.Stat(*envPath); os.IsNotExist(statErr) {
			if err = setup(ctx, *envPath); err == nil {
				err = run(ctx, *envPath, "run")
			}
		} else {
			err = run(ctx, *envPath, "run")
		}
	case "setup":
		err = setup(ctx, *envPath)
	case "run", "once", "dry-run":
		err = run(ctx, *envPath, command)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("pgsync: %v", err)
	}
}

// ---- database helpers ----------------------------------------------------

func mysqlDSN(user, pass, host, port, name string) string {
	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = pass
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(host, port)
	cfg.DBName = name
	cfg.Timeout = 10 * time.Second
	cfg.ReadTimeout = 2 * time.Minute
	cfg.WriteTimeout = 2 * time.Minute
	cfg.Params = map[string]string{"charset": "utf8mb4"}
	return cfg.FormatDSN()
}

func openDB(ctx context.Context, user, pass, host, port, name string) (*sql.DB, error) {
	db, err := sql.Open("mysql", mysqlDSN(user, pass, host, port, name))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(10 * time.Minute)
	pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func quoteIdent(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

// openStateDB creates the script database if needed and its table.
func openStateDB(ctx context.Context, v map[string]string) (*sql.DB, error) {
	server, err := openDB(ctx, v[keyStateUser], v[keyStatePass], v[keyStateHost], v[keyStatePort], "")
	if err != nil {
		return nil, err
	}
	_, err = server.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(v[keyStateName])+" CHARACTER SET utf8mb4")
	server.Close()
	if err != nil {
		return nil, fmt.Errorf("create database %s: %w", v[keyStateName], err)
	}
	db, err := openDB(ctx, v[keyStateUser], v[keyStatePass], v[keyStateHost], v[keyStatePort], v[keyStateName])
	if err != nil {
		return nil, err
	}
	if err := (syncer.StateDB{DB: db}).Migrate(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("create table: %w", err)
	}
	return db, nil
}

func openRebeccaDB(ctx context.Context, v map[string]string) (*sql.DB, int, error) {
	db, err := openDB(ctx, v[keyRebUser], v[keyRebPass], v[keyRebHost], v[keyRebPort], v[keyRebName])
	if err != nil {
		return nil, 0, err
	}
	var users int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&users); err != nil {
		db.Close()
		return nil, 0, fmt.Errorf("read users table: %w", err)
	}
	var creds int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM user_credentials").Scan(&creds); err != nil {
		db.Close()
		return nil, 0, fmt.Errorf("read user_credentials table (is this Rebecca's database?): %w", err)
	}
	return db, users, nil
}

// ---- interactive setup -----------------------------------------------------

type prompter struct{ in *bufio.Reader }

func (p prompter) ask(label, current string) string {
	if current != "" {
		fmt.Printf("%s [%s]: ", label, current)
	} else {
		fmt.Printf("%s: ", label)
	}
	line, _ := p.in.ReadString('\n')
	line = strings.TrimSpace(line)
	if line == "" {
		return current
	}
	return line
}

func (p prompter) secret(label string, current string) string {
	hint := ""
	if current != "" {
		hint = " [Enter = keep current]"
	}
	fmt.Printf("%s%s: ", label, hint)
	echoOff := exec.Command("stty", "-echo")
	echoOff.Stdin = os.Stdin
	hidden := echoOff.Run() == nil
	line, _ := p.in.ReadString('\n')
	if hidden {
		echoOn := exec.Command("stty", "echo")
		echoOn.Stdin = os.Stdin
		_ = echoOn.Run()
		fmt.Println()
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return current
	}
	return line
}

func withDefault(v map[string]string, key, value string) {
	if strings.TrimSpace(v[key]) == "" {
		v[key] = value
	}
}

func setup(ctx context.Context, envPath string) error {
	v, err := syncer.ReadEnvFile(envPath)
	if err != nil {
		return err
	}
	p := prompter{in: bufio.NewReader(os.Stdin)}

	// Rebecca's own settings give good defaults for the database host/user.
	rebeccaURL := syncer.DatabaseURL{}
	if rebeccaEnv, err := syncer.ReadEnvFile(rebeccaEnvFile); err == nil {
		if parsed, ok := syncer.ParseDatabaseURL(rebeccaEnv["SQLALCHEMY_DATABASE_URL"]); ok {
			rebeccaURL = parsed
		}
	}
	withDefault(v, keyStateHost, firstNonEmpty(rebeccaURL.Host, "127.0.0.1"))
	withDefault(v, keyStatePort, firstNonEmpty(rebeccaURL.Port, "3306"))
	withDefault(v, keyStateName, "pgsync")
	withDefault(v, keyPanelURL, defaultPanelURL)
	withDefault(v, keyRebHost, firstNonEmpty(rebeccaURL.Host, "127.0.0.1"))
	withDefault(v, keyRebPort, firstNonEmpty(rebeccaURL.Port, "3306"))
	withDefault(v, keyRebName, firstNonEmpty(rebeccaURL.Name, "rebecca"))
	withDefault(v, keyRebUser, rebeccaURL.User)
	withDefault(v, keyRebPass, rebeccaURL.Password)
	withDefault(v, keyInterval, defaultInterval)
	withDefault(v, keyLimitGB, defaultLimitGB)
	withDefault(v, keyResetGB, defaultResetGB)
	withDefault(v, keyConcurrency, defaultConcurrent)

	fmt.Println("== 1/3  Script database (MySQL) ==")
	for {
		v[keyStateUser] = p.ask("Database username", v[keyStateUser])
		v[keyStatePass] = p.secret("Database password", v[keyStatePass])
		v[keyStateHost] = p.ask("Database host", v[keyStateHost])
		v[keyStatePort] = p.ask("Database port", v[keyStatePort])
		v[keyStateName] = p.ask("Database name for the script", v[keyStateName])
		fmt.Print("Testing connection... ")
		db, err := openStateDB(ctx, v)
		if err == nil {
			db.Close()
			fmt.Println("OK")
			break
		}
		fmt.Printf("FAILED: %v\nTry again.\n", err)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	fmt.Println("\n== 2/3  PasarGuard panel ==")
	for {
		v[keyPanelURL] = strings.TrimRight(p.ask("Panel address", v[keyPanelURL]), "/")
		v[keyPanelURL] = strings.TrimSuffix(v[keyPanelURL], "/dashboard")
		v[keyPanelUser] = p.ask("Panel admin username", v[keyPanelUser])
		v[keyPanelPass] = p.secret("Panel admin password", v[keyPanelPass])
		fmt.Print("Testing login... ")
		client := syncer.NewPanelClient(v[keyPanelURL], v[keyPanelUser], v[keyPanelPass])
		testCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		err := client.Login(testCtx)
		var groups []int64
		if err == nil {
			groups, err = client.GroupIDs(testCtx)
		}
		cancel()
		if err == nil && len(groups) == 0 {
			err = errors.New("logged in, but the panel has no groups")
		}
		if err == nil {
			fmt.Printf("OK (%d groups)\n", len(groups))
			break
		}
		fmt.Printf("FAILED: %v\nTry again.\n", err)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	fmt.Println("\n== 3/3  Rebecca database ==")
	for {
		v[keyRebUser] = p.ask("Rebecca database username", v[keyRebUser])
		v[keyRebPass] = p.secret("Rebecca database password", v[keyRebPass])
		v[keyRebHost] = p.ask("Rebecca database host", v[keyRebHost])
		v[keyRebPort] = p.ask("Rebecca database port", v[keyRebPort])
		v[keyRebName] = p.ask("Rebecca database name", v[keyRebName])
		fmt.Print("Testing connection... ")
		db, users, err := openRebeccaDB(ctx, v)
		if err == nil {
			db.Close()
			fmt.Printf("OK (%d users)\n", users)
			break
		}
		fmt.Printf("FAILED: %v\nTry again.\n", err)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	if dir := filepath.Dir(envPath); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	if err := os.WriteFile(envPath, []byte(syncer.FormatEnv(v, envOrder)), 0o600); err != nil {
		return err
	}
	_ = os.Chmod(envPath, 0o600)
	fmt.Printf("\nSettings saved to %s\n", envPath)
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

// ---- sync loop -------------------------------------------------------------

func intSetting(v map[string]string, key string, fallback int64) int64 {
	if n, err := strconv.ParseInt(strings.TrimSpace(v[key]), 10, 64); err == nil && n > 0 {
		return n
	}
	return fallback
}

func run(ctx context.Context, envPath string, mode string) error {
	v, err := syncer.ReadEnvFile(envPath)
	if err != nil {
		return err
	}
	if v[keyStateUser] == "" || v[keyPanelURL] == "" || v[keyRebUser] == "" {
		return fmt.Errorf("settings missing in %s; run: pgsync setup", envPath)
	}
	stateDB, err := openStateDB(ctx, v)
	if err != nil {
		return fmt.Errorf("script database: %w", err)
	}
	defer stateDB.Close()
	rebeccaDB, users, err := openRebeccaDB(ctx, v)
	if err != nil {
		return fmt.Errorf("rebecca database: %w", err)
	}
	defer rebeccaDB.Close()

	const gb = int64(1 << 30)
	engine := &syncer.Engine{
		Panel:   syncer.NewPanelClient(v[keyPanelURL], v[keyPanelUser], v[keyPanelPass]),
		Rebecca: syncer.RebeccaDB{DB: rebeccaDB},
		State:   syncer.StateDB{DB: stateDB},
		Opt: syncer.Options{
			DataLimit:   intSetting(v, keyLimitGB, 300) * gb,
			ResetAt:     intSetting(v, keyResetGB, 250) * gb,
			Concurrency: int(intSetting(v, keyConcurrency, 8)),
			DryRun:      mode == "dry-run",
			Logf:        log.Printf,
		},
	}
	interval := time.Duration(intSetting(v, keyInterval, 60)) * time.Second
	log.Printf("pgsync started: %d Rebecca users, panel %s, every %s", users, v[keyPanelURL], interval)

	cycle := func() {
		cycleCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
		defer cancel()
		stats, err := engine.RunOnce(cycleCtx)
		prefix := ""
		if mode == "dry-run" {
			prefix = "[dry-run, nothing changed] "
		}
		if err != nil {
			log.Printf("%scycle failed: %v", prefix, err)
			return
		}
		log.Printf("%scycle ok in %s: rebecca=%d panel=%d created=%d adopted=%d updated=%d disabled=%d resets=%d usage=%d users/%.2f GB skipped=%d errors=%d",
			prefix, stats.Duration.Round(time.Millisecond), stats.RebeccaUsers, stats.PanelUsers, stats.Created, stats.Adopted,
			stats.Modified, stats.Disabled, stats.Resets, stats.UsageUsers, float64(stats.UsageBytes)/float64(gb), stats.Skipped, stats.Errors)
	}

	if mode != "run" {
		cycle()
		return nil
	}
	for {
		started := time.Now()
		cycle()
		wait := interval - time.Since(started)
		if wait < time.Second {
			wait = time.Second
		}
		select {
		case <-ctx.Done():
			log.Printf("pgsync stopped")
			return nil
		case <-time.After(wait):
		}
	}
}
