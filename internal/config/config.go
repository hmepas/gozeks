// Package config loads the gozeks TOML configuration and applies defaults.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// Duration is a time.Duration that unmarshals from a TOML string such as "5s".
type Duration struct{ time.Duration }

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

// Config is the whole configuration file.
type Config struct {
	Node     string `toml:"node"`
	Root     string `toml:"root"`
	StateDir string `toml:"state_dir"`
	S3       S3     `toml:"s3"`
	Sync     Sync   `toml:"sync"`
	Relay    Relay  `toml:"relay"`
}

// S3 is the bucket connection. Keys come from the environment only.
type S3 struct {
	Endpoint  string `toml:"endpoint"`
	Region    string `toml:"region"`
	Bucket    string `toml:"bucket"`
	PathStyle bool   `toml:"path_style"`
	AccessKey string `toml:"-"`
	SecretKey string `toml:"-"`
}

// Sync holds timing knobs and the exclude list.
type Sync struct {
	Poll         Duration `toml:"poll"`
	IdlePoll     Duration `toml:"idle_poll"`
	IdleAfter    Duration `toml:"idle_after"`
	Debounce     Duration `toml:"debounce"`
	MaxBatchWait Duration `toml:"max_batch_wait"`
	Reconcile    Duration `toml:"reconcile"`
	FallbackPoll Duration `toml:"fallback_poll"`
	Exclude      []string `toml:"exclude"`
}

// Relay holds the relay role settings. Token comes from the environment only.
type Relay struct {
	Listen    string   `toml:"listen"`
	Advertise string   `toml:"advertise"`
	Lease     Duration `toml:"lease"`
	Token     string   `toml:"-"`
}

// DefaultExcludes are the patterns applied when the config does not set sync.exclude.
var DefaultExcludes = []string{
	".obsidian/workspace*.json",
	".obsidian/workspace",
	".obsidian/cache/",
	".trash/",
	".zk/",
	".DS_Store",
	"*.tmp",
	"*~",
	".#*",
	".gozeks-*",
}

// DefaultPath is the config file location used when -config is not given.
func DefaultPath() string {
	return filepath.Join(home(), ".config", "gozeks", "config.toml")
}

// Load reads the file, fills defaults and pulls secrets from the environment.
func Load(path string) (*Config, error) {
	c := &Config{}
	if _, err := toml.DecodeFile(path, c); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	if err := c.finish(); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return c, nil
}

func (c *Config) finish() error {
	if c.Node == "" {
		return errors.New("node is required")
	}
	if c.Root == "" {
		return errors.New("root is required")
	}
	c.Root = expand(c.Root)
	if c.StateDir == "" {
		c.StateDir = filepath.Join(home(), ".local", "state", "gozeks", filepath.Base(c.Root))
	}
	c.StateDir = expand(c.StateDir)
	if c.S3.Endpoint == "" || c.S3.Bucket == "" {
		return errors.New("s3.endpoint and s3.bucket are required")
	}
	if c.S3.Region == "" {
		c.S3.Region = "us-east-1"
	}
	c.S3.AccessKey = firstEnv("GOZEKS_S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID")
	c.S3.SecretKey = firstEnv("GOZEKS_S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY")
	if c.S3.AccessKey == "" || c.S3.SecretKey == "" {
		return errors.New("GOZEKS_S3_ACCESS_KEY / GOZEKS_S3_SECRET_KEY not set")
	}
	c.Relay.Token = os.Getenv("GOZEKS_RELAY_TOKEN")
	if c.Relay.Listen != "" && c.Relay.Advertise == "" {
		return errors.New("relay.advertise is required when relay.listen is set")
	}

	def := func(d *Duration, v time.Duration) {
		if d.Duration == 0 {
			d.Duration = v
		}
	}
	def(&c.Sync.Poll, 5*time.Second)
	def(&c.Sync.IdlePoll, c.Sync.Poll.Duration)
	def(&c.Sync.IdleAfter, 10*time.Minute)
	def(&c.Sync.Debounce, 2*time.Second)
	def(&c.Sync.MaxBatchWait, 5*time.Second)
	def(&c.Sync.Reconcile, 10*time.Minute)
	def(&c.Sync.FallbackPoll, 60*time.Second)
	def(&c.Relay.Lease, 10*time.Minute)
	if c.Sync.Exclude == nil {
		c.Sync.Exclude = append([]string(nil), DefaultExcludes...)
	}
	return nil
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

func home() string {
	h, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	return h
}

func expand(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		p = filepath.Join(home(), strings.TrimPrefix(p, "~"))
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return abs
}
