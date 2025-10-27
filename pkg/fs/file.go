package fs

import (
	"os"
	"runtime"
	"strings"
)

// FileExists - check if a file exists locally
func FileExists(path string) bool {
	inf, err := os.Stat(path)

	// Check if a file is bigger than 4 bytes
	if inf != nil && inf.Size() < 5 {
		return false
	}

	// Check if error is nil or not exists
	return err == nil || os.IsExist(err)
}
func ModuleRootPath() string {
	_, filename, _, _ := runtime.Caller(0)
	root := strings.Split(filename, "/")
	var paths []string
	for _, path := range root {
		if path == "pkg" || path == "internal" || strings.HasSuffix(path, ".go") {
			break
		}
		paths = append(paths, path)
	}
	p := strings.Join(paths, "/")
	return p
}
