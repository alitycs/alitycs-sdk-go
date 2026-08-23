package alitycs

import (
	"os"
	"runtime"
	"strings"
	"time"
)

// collectContext gathers the environment an event was emitted from.
func collectContext() Context {
	return Context{
		SDKVersion:  Version,
		SDKLanguage: "go",
		Locale:      localeFromEnvironment(),
		Timezone:    timezoneName(time.Local),
		OSName:      runtime.GOOS,
		OSArch:      runtime.GOARCH,
		GoVersion:   runtime.Version(),
	}
}

func timezoneName(loc *time.Location) string {
	if loc == nil {
		return ""
	}
	return loc.String()
}

// localeFromEnvironment normalises $LC_ALL/$LANG (e.g. en_US.UTF-8) into a
// BCP 47 language tag (en-US). Returns "" when no locale is configured.
func localeFromEnvironment() string {
	for _, key := range []string{"LC_ALL", "LC_MESSAGES", "LANG"} {
		if tag := normalizeLocale(os.Getenv(key)); tag != "" {
			return tag
		}
	}
	return ""
}

func normalizeLocale(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || value == "C" || value == "POSIX" || strings.HasPrefix(value, "C.") {
		return ""
	}
	if index := strings.IndexByte(value, '.'); index >= 0 {
		value = value[:index]
	}
	tag := strings.ReplaceAll(value, "_", "-")
	if len(tag) < 2 {
		return ""
	}
	return tag
}
