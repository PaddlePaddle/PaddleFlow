package utils

import (
	"path/filepath"
	"runtime"
	"testing"
)

type PathTest struct {
	path, result string
}

var cleantests = []PathTest{
	// Already clean
	{"abc", "/abc"},
	{"abc/def", "/abc/def"},
	{"a/b/c", "/a/b/c"},
	{".", "/"},
	{"..", "/"},
	{"../..", "/"},
	{"../../abc", "/abc"},
	{"/abc", "/abc"},
	{"/", "/"},

	// Empty is current dir
	{"", "/"},

	// Remove trailing slash
	{"abc/", "/abc"},
	{"abc/def/", "/abc/def"},
	{"a/b/c/", "/a/b/c"},
	{"./", "/"},
	{"../", "/"},
	{"../../", "/"},
	{"/abc/", "/abc"},
	{"./abc/", "/abc"},
	{"./abc/def", "/abc/def"},

	// Remove doubled slash
	{"abc//def//ghi", "/abc/def/ghi"},
	{"//abc", "/abc"},
	{"///abc", "/abc"},
	{"//abc//", "/abc"},
	{"abc//", "/abc"},

	// Remove . elements
	{"abc/./def", "/abc/def"},
	{"/./abc/def", "/abc/def"},
	{"abc/.", "/abc"},

	// Remove .. elements
	{"abc/def/ghi/../jkl", "/abc/def/jkl"},
	{"abc/def/../ghi/../jkl", "/abc/jkl"},
	{"abc/def/..", "/abc"},
	{"abc/def/../..", "/"},
	{"/abc/def/../..", "/"},
	{"abc/def/../../..", "/"},
	{"/abc/def/../../..", "/"},
	{"abc/def/../../../ghi/jkl/../../../mno", "/mno"},
	{"/../abc", "/abc"},

	// Combinations
	{"abc/./../def", "/def"},
	{"abc//./../def", "/def"},
	{"abc/../../././../def", "/def"},

	// root dir
	{"./../def", "/def"},
	{"../../def", "/def"},

	{"/..", "/"},
}

var wincleantests = []PathTest{
	{`c:`, `c:.`},
	{`c:\`, `c:\`},
	{`c:\abc`, `c:\abc`},
	{`c:abc\..\..\.\.\..\def`, `c:..\..\def`},
	{`c:\abc\def\..\..`, `c:\`},
	{`c:\..\abc`, `c:\abc`},
	{`c:..\abc`, `c:..\abc`},
	{`\`, `\`},
	{`/`, `\`},
	{`\\i\..\c$`, `\c$`},
	{`\\i\..\i\c$`, `\i\c$`},
	{`\\i\..\I\c$`, `\I\c$`},
	{`\\host\share\foo\..\bar`, `\\host\share\bar`},
	{`//host/share/foo/../baz`, `\\host\share\baz`},
	{`\\a\b\..\c`, `\\a\b\c`},
	{`\\a\b`, `\\a\b`},
}

func TestClean(t *testing.T) {
	tests := cleantests
	if runtime.GOOS == "windows" {
		for i := range tests {
			tests[i].result = filepath.FromSlash(tests[i].result)
		}
		tests = append(tests, wincleantests...)
	}
	for index, test := range tests {
		if s := MountPathClean(test.path); s != test.result {
			t.Errorf("No.%d path   Clean(%q) = %q, want %q", index, test.path, s, test.result)
		}
		if s := MountPathClean(test.result); s != test.result {
			t.Errorf("No.%d result Clean(%q) = %q, want %q", index, test.result, s, test.result)
		}
	}
}
