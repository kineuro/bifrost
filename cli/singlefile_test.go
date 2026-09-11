package main

import (
	"os"
	"path/filepath"
	"testing"
)

// A single file is indexed under its own name and has to be opened from the folder that holds it. Before
// fileBase the name was joined to the file's own path, "marker/marker", and every attempt failed with
// "not a directory" (the TRANSFER-COMPLETE marker of 2026-09-11 never went).
func TestSingleFileRootIsOpenedFromItsFolder(t *testing.T) {
	root := tree(t, map[string]int{"only.dcm": 9})
	file := filepath.Join(root, "only.dcm")
	got := collect(t, file, nil)
	if len(got) != 1 || got[0].rel != "only.dcm" {
		t.Fatalf("single file root: %v", got)
	}
	p := got[0].abs(fileBase(file))
	if p != file {
		t.Fatalf("a single file is opened at %q, want %q", p, file)
	}
	st, err := os.Stat(p)
	if err != nil || st.Size() != 9 {
		t.Fatalf("the single file cannot be opened where it is looked for: %v", err)
	}
}

func TestJoinRemoteTreatsSlashAsTheRoot(t *testing.T) {
	for _, c := range []struct{ prefix, rel, want string }{
		{"", "a.dcm", "a.dcm"},
		{"/", "a.dcm", "a.dcm"},
		{"//", "sub/a.dcm", "sub/a.dcm"},
		{"inner", "a.dcm", "inner/a.dcm"},
		{"/inner/", "a.dcm", "inner/a.dcm"},
		{`in\ner`, "a.dcm", "in/ner/a.dcm"},
	} {
		if got := joinRemote(c.prefix, c.rel); got != c.want {
			t.Errorf("joinRemote(%q, %q) = %q, want %q", c.prefix, c.rel, got, c.want)
		}
	}
}

func TestFolderRootIsOpenedFromItself(t *testing.T) {
	root := tree(t, map[string]int{"a/b.dcm": 3})
	if got := fileBase(root); got != root {
		t.Fatalf("fileBase(folder) = %q, want the folder itself", got)
	}
	if got := fileBase(filepath.Join(root, "missing")); got != filepath.Join(root, "missing") {
		t.Fatalf("fileBase of a path that does not exist should leave it alone, got %q", got)
	}
}
