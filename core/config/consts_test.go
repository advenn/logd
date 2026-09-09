package config_test

import (
	"testing"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/storage"
)

// core/config is a dependency-free leaf, so the compressed-block default is duplicated
// there rather than imported from core/storage. This test is what keeps the copy honest.
//
// It lives in the EXTERNAL test package on purpose: a test-only import does not appear in
// the library's dependency graph, so config stays a leaf while the constants stay pinned
// together. Without this, the two could drift and a deployment would silently use a
// different block size than the documentation claims.
func TestDefaultBlockPagesMatchesStorage(t *testing.T) {
	if got, want := (&config.Config{}).BlockPages(), storage.DefaultBlockPages; got != want {
		t.Fatalf("config default block pages = %d, storage.DefaultBlockPages = %d — the duplicated constant has drifted", got, want)
	}
}
