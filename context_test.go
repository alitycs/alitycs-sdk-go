package alitycs

import (
	"runtime"
	"testing"
)

func TestCollectContextRequiredFields(t *testing.T) {
	t.Setenv("LANG", "en_US.UTF-8")
	context := collectContext()

	if context.SDKVersion != Version {
		t.Errorf("sdkVersion = %q, want %q", context.SDKVersion, Version)
	}
	if context.SDKLanguage != "go" {
		t.Errorf("sdkLanguage = %q", context.SDKLanguage)
	}
	if context.OSName != runtime.GOOS || context.OSArch != runtime.GOARCH {
		t.Errorf("os = %s/%s, want %s/%s", context.OSName, context.OSArch, runtime.GOOS, runtime.GOARCH)
	}
	if context.GoVersion == "" {
		t.Errorf("goVersion missing")
	}
}

func TestLocaleFromEnvironmentPrecedence(t *testing.T) {
	t.Setenv("LC_ALL", "de_DE.UTF-8")
	t.Setenv("LC_MESSAGES", "fr_FR")
	t.Setenv("LANG", "en_GB")
	if got := localeFromEnvironment(); got != "de-DE" {
		t.Errorf("locale = %q, want de-DE (LC_ALL wins)", got)
	}
}

func TestNormalizeLocale(t *testing.T) {
	cases := map[string]string{
		"en_US.UTF-8": "en-US",
		"de_DE":       "de-DE",
		"C":           "",
		"C.UTF-8":     "",
		"POSIX":       "",
		"":            "",
		"  ":          "",
		"x":           "",
	}
	for input, want := range cases {
		if got := normalizeLocale(input); got != want {
			t.Errorf("normalizeLocale(%q) = %q, want %q", input, got, want)
		}
	}
}
