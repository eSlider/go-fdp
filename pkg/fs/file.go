package fs

import "os"

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
