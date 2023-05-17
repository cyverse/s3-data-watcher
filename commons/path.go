package commons

import (
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/xerrors"
)

func ExpandHomeDir(path string) (string, error) {
	// resolve "~/"
	if path == "~" {
		homedir, err := os.UserHomeDir()
		if err != nil {
			return "", xerrors.Errorf("failed to get user home dir: %w", err)
		}

		return homedir, nil
	} else if strings.HasPrefix(path, "~/") {
		homedir, err := os.UserHomeDir()
		if err != nil {
			return "", xerrors.Errorf("failed to get user home dir: %w", err)
		}

		path = filepath.Join(homedir, path[2:])
		return filepath.Clean(path), nil
	}

	return path, nil
}
