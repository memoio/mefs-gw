package utils

import (
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/mitchellh/go-homedir"
)

const GATEWAYPATH = "GATEWAYPATH"

var DefaultPathRoot = "~/.mefs_gw"

// BestKnownPath returns the best known fsrepo path. If the ENV override is
// present, this function returns that value. Otherwise, it returns the default
// repo path.
func BestKnownPath() (string, error) {
	mefsPath := DefaultPathRoot
	if os.Getenv(GATEWAYPATH) != "" { //获取环境变量
		mefsPath = os.Getenv(GATEWAYPATH)
	}
	mefsPath, err := homedir.Expand(mefsPath)
	if err != nil {
		return "", err
	}

	_, err = os.Stat(mefsPath)
	if os.IsNotExist(err) {
		err = os.Mkdir(mefsPath, 0755)
		if err != nil {
			return "", err
		}
	}
	return mefsPath, nil
}

const globalWindowsOSName = "windows"

// HasSuffix - Suffix matcher string matches suffix in a platform specific way.
// For example on windows since its case insensitive we are supposed
// to do case insensitive checks.
func HasSuffix(s string, suffix string) bool {
	if runtime.GOOS == globalWindowsOSName {
		return strings.HasSuffix(strings.ToLower(s), strings.ToLower(suffix))
	}
	return strings.HasSuffix(s, suffix)
}

// SlashSeparator - slash separator.
const SlashSeparator = "/"

// pathJoin - like path.Join() but retains trailing SlashSeparator of the last element
func pathJoin(elem ...string) string {
	trailingSlash := ""
	if len(elem) > 0 {
		if HasSuffix(elem[len(elem)-1], SlashSeparator) {
			trailingSlash = SlashSeparator
		}
	}
	return path.Join(elem...) + trailingSlash
}
