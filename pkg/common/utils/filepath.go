package utils

import (
	"path/filepath"
	"runtime"
	"strings"
)

// MountPathClean cleans the path for mountPath
func MountPathClean(mountPath string) string {
	mountPath = filepath.Clean(mountPath)
	if runtime.GOOS == "windows" {
		return mountPath
	}
	for strings.HasSuffix(mountPath, "../") {
		mountPath = strings.TrimSuffix(mountPath, "../")
	}
	if !strings.HasPrefix(mountPath, "/") {
		mountPath = filepath.Join("/", mountPath)
	}
	return mountPath
}
