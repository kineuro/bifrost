package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// tree writes files under a fresh directory. Keys are relative paths with forward slashes.
func tree(t *testing.T, files map[string]int) string {
	t.Helper()
	root := t.TempDir()
	for rel, size := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func collect(t *testing.T, root string, globs []string) []localFile {
	t.Helper()
	got, err := index(context.Background(), root, globs, newProgress(true))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestIndexFindsEveryFileSorted(t *testing.T) {
	root := tree(t, map[string]int{
		"b/2.dcm":     10,
		"a/1.dcm":     20,
		"a/deep/x/y":  5,
		"top.txt":     1,
		"a/deep/z.md": 7,
	})
	got := collect(t, root, nil)
	want := []string{"a/1.dcm", "a/deep/x/y", "a/deep/z.md", "b/2.dcm", "top.txt"}
	if len(got) != len(want) {
		t.Fatalf("got %d files, want %d: %v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].rel != w {
			t.Errorf("position %d: got %q, want %q", i, got[i].rel, w)
		}
	}
	if got[0].size != 20 {
		t.Errorf("size not carried: got %d, want 20", got[0].size)
	}
}

func TestIndexHonoursExcludes(t *testing.T) {
	root := tree(t, map[string]int{"keep.dcm": 1, "drop.tmp": 1, "sub/also.tmp": 1})
	got := collect(t, root, []string{"*.tmp"})
	if len(got) != 1 || got[0].rel != "keep.dcm" {
		t.Fatalf("excludes not applied: %v", got)
	}
}

func TestIndexSkipsSymlinks(t *testing.T) {
	root := tree(t, map[string]int{"real.dcm": 3})
	if err := os.Symlink(filepath.Join(root, "real.dcm"), filepath.Join(root, "link.dcm")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	got := collect(t, root, nil)
	if len(got) != 1 || got[0].rel != "real.dcm" {
		t.Fatalf("symlink not skipped: %v", got)
	}
}

// A single file as the root is a shape the walk has to answer for too, because push accepts one.
func TestIndexSingleFileRoot(t *testing.T) {
	root := tree(t, map[string]int{"only.dcm": 9})
	got := collect(t, filepath.Join(root, "only.dcm"), nil)
	if len(got) != 1 || got[0].rel != "only.dcm" || got[0].size != 9 {
		t.Fatalf("single file root: %v", got)
	}
}

// The walk hands work between a fixed pool of goroutines and has to decide for itself when the tree is
// exhausted. A wide, deep, partly empty tree is where that decision goes wrong, so it is the one to test.
func TestWalkTerminatesOnWideDeepTree(t *testing.T) {
	files := map[string]int{}
	for i := 0; i < 60; i++ {
		for j := 0; j < 5; j++ {
			files[fmt.Sprintf("s%02d/ses%d/im.dcm", i, j)] = 1
		}
	}
	root := tree(t, files)
	// Directories holding nothing but other directories, and some holding nothing at all.
	for i := 0; i < 40; i++ {
		if err := os.MkdirAll(filepath.Join(root, fmt.Sprintf("empty%02d/a/b/c", i)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	got := collect(t, root, nil)
	if len(got) != len(files) {
		t.Fatalf("got %d files, want %d", len(got), len(files))
	}
}

func TestCountMatchesIndex(t *testing.T) {
	files := map[string]int{}
	var wantBytes int64
	for i := 0; i < 250; i++ {
		files[fmt.Sprintf("d%d/f%d.dcm", i%7, i)] = i
		wantBytes += int64(i)
	}
	root := tree(t, files)
	n, b, err := count(context.Background(), root, nil, newProgress(true))
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(files)) || b != wantBytes {
		t.Fatalf("count = %d files/%d bytes, want %d/%d", n, b, len(files), wantBytes)
	}
	if got := collect(t, root, nil); int64(len(got)) != n {
		t.Fatalf("count %d disagrees with index %d", n, len(got))
	}
}

// Chunking is the whole point of the change: every file exactly once, and no chunk larger than asked for.
func TestIndexChunksCoverTreeExactlyOnce(t *testing.T) {
	files := map[string]int{}
	for i := 0; i < 500; i++ {
		files[fmt.Sprintf("sub%02d/f%03d.dcm", i%20, i)] = 1
	}
	root := tree(t, files)
	for _, chunk := range []int{1, 7, 64, 499, 500, 501, 10000} {
		ch := make(chan []localFile, 1)
		errc := make(chan error, 1)
		go func(n int) {
			errc <- indexChunks(context.Background(), root, nil, n, newProgress(true), ch)
		}(chunk)
		seen := map[string]int{}
		nchunks := 0
		for c := range ch {
			nchunks++
			if len(c) > chunk {
				t.Errorf("chunk=%d: got a chunk of %d", chunk, len(c))
			}
			if !sort.SliceIsSorted(c, func(i, j int) bool { return c[i].rel < c[j].rel }) {
				t.Errorf("chunk=%d: chunk is not sorted", chunk)
			}
			for _, f := range c {
				seen[f.rel]++
			}
		}
		if err := <-errc; err != nil {
			t.Fatalf("chunk=%d: %v", chunk, err)
		}
		if len(seen) != len(files) {
			t.Errorf("chunk=%d: saw %d distinct files, want %d", chunk, len(seen), len(files))
		}
		for rel, n := range seen {
			if n != 1 {
				t.Errorf("chunk=%d: %s delivered %d times", chunk, rel, n)
			}
		}
		if nchunks == 0 {
			t.Errorf("chunk=%d: no chunks delivered", chunk)
		}
	}
}

// A cancelled push must not leave the walk blocked on a send with nobody reading.
func TestIndexChunksStopsOnCancel(t *testing.T) {
	files := map[string]int{}
	for i := 0; i < 2000; i++ {
		files[fmt.Sprintf("d%02d/f%04d.dcm", i%25, i)] = 1
	}
	root := tree(t, files)
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan []localFile, 1)
	errc := make(chan error, 1)
	go func() { errc <- indexChunks(ctx, root, nil, 8, newProgress(true), ch) }()
	<-ch // take one chunk, then walk away
	cancel()
	for range ch {
	}
	if err := <-errc; err != nil && err != context.Canceled {
		t.Fatalf("unexpected error after cancel: %v", err)
	}
}

func TestAbsRebuildsThePath(t *testing.T) {
	f := localFile{rel: "a/b/c.dcm"}
	want := filepath.Join("/data/root", "a", "b", "c.dcm")
	if got := f.abs("/data/root"); got != want {
		t.Fatalf("abs = %q, want %q", got, want)
	}
}

func TestMakeBatchesRespectsLimits(t *testing.T) {
	var files []*localFile
	for i := 0; i < 10; i++ {
		files = append(files, &localFile{rel: fmt.Sprintf("f%d", i), size: 30})
	}
	byFiles := makeBatches(files, 1<<30, 3)
	for _, b := range byFiles {
		if len(b) > 3 {
			t.Fatalf("batch of %d files, limit 3", len(b))
		}
	}
	byBytes := makeBatches(files, 100, 1000)
	for _, b := range byBytes {
		var n int64
		for _, f := range b {
			n += f.size
		}
		if len(b) > 1 && n > 100 {
			t.Fatalf("batch of %d bytes, limit 100", n)
		}
	}
	var total int
	for _, b := range byBytes {
		total += len(b)
	}
	if total != len(files) {
		t.Fatalf("batches hold %d files, want %d", total, len(files))
	}
}

func TestPlanFileOmitsAnEmptyHash(t *testing.T) {
	b, err := json.Marshal(planFile{Path: "a/b.dcm", Size: 5})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "sha256") {
		t.Fatalf("empty hash should not be sent: %s", b)
	}
	b, err = json.Marshal(planFile{Path: "a/b.dcm", Size: 5, Sha256: "abc"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "abc") {
		t.Fatalf("hash should be sent when known: %s", b)
	}
}
