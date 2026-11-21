package store

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/hmepas/gozeks/internal/config"
)

// Some providers answer 409 OperationAborted ("Try again.") to a GET that collides
// with a PUT of the same key; the client must retry it instead of failing.
func TestGetRetriesOperationAborted(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>OperationAborted</Code>` +
				`<Message>A conflicting conditional operation is currently in progress against this resource. Try again.</Message></Error>`))
			return
		}
		w.Header().Set("ETag", `"abc"`)
		w.Write([]byte("hello"))
	}))
	defer srv.Close()

	st, err := New(config.S3{Endpoint: srv.URL, Region: "x", Bucket: "b", PathStyle: true, AccessKey: "k", SecretKey: "s"})
	if err != nil {
		t.Fatal(err)
	}
	body, _, _, err := st.Get(context.Background(), "data/n.md")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(body) != "hello" || calls.Load() != 2 {
		t.Fatalf("body=%q calls=%d", body, calls.Load())
	}
}
