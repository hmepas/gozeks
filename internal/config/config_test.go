package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnvFile(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "config.toml"), []byte(`
node = "t"
root = "`+dir+`"
[s3]
endpoint = "https://s3.example"
bucket = "b"
`), 0o600)
	os.WriteFile(filepath.Join(dir, "env"), []byte(`
# comment
export GOZEKS_S3_ACCESS_KEY="from-file"
GOZEKS_S3_SECRET_KEY='sec'
GOZEKS_RELAY_TOKEN=tok
junk line
`), 0o600)
	t.Setenv("GOZEKS_S3_ACCESS_KEY", "from-env")
	os.Unsetenv("GOZEKS_S3_SECRET_KEY")
	os.Unsetenv("GOZEKS_RELAY_TOKEN")
	t.Cleanup(func() { os.Unsetenv("GOZEKS_S3_SECRET_KEY"); os.Unsetenv("GOZEKS_RELAY_TOKEN") })

	c, err := Load(filepath.Join(dir, "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if c.S3.AccessKey != "from-env" {
		t.Errorf("process env must win, got %q", c.S3.AccessKey)
	}
	if c.S3.SecretKey != "sec" || c.Relay.Token != "tok" {
		t.Errorf("env file not applied: %q %q", c.S3.SecretKey, c.Relay.Token)
	}
	if err := c.CheckCredentials(); err != nil {
		t.Error(err)
	}
}

func TestMissingCredentialsDeferred(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "config.toml"), []byte(`
node = "t"
root = "`+dir+`"
[s3]
endpoint = "https://s3.example"
bucket = "b"
`), 0o600)
	for _, k := range []string{"GOZEKS_S3_ACCESS_KEY", "GOZEKS_S3_SECRET_KEY", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
	c, err := Load(filepath.Join(dir, "config.toml"))
	if err != nil {
		t.Fatal("load must succeed without credentials:", err)
	}
	if c.CheckCredentials() == nil {
		t.Fatal("expected ErrNoCredentials")
	}
}
