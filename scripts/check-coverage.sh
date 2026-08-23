#!/bin/sh
# Coverage gate for alitycs-sdk-go.
#
# Runs the suite, then enforces the platform bar from
# .agents/goals/tier-1-platform-sdks.md: total statement coverage >= 90% and
# >= 85% of functions carrying non-zero coverage. Exits non-zero below either
# number — this is what `conformance coverage` shells out to.
set -eu
cd "$(dirname "$0")/.."

# Scope the gate to the SDK library packages. The e2e/ driver binary is an
# external-process fixture (the JVM SDK's E2eMain.kt plays the same role
# outside Kover's measurement); including its untested main() would make the
# number say less, not more.
packages=$(go list ./... | grep -v '/e2e$')

go test -coverprofile=cover.out ${packages}
go tool cover -func=cover.out | awk -v min_statements=90 -v min_functions=85 '
	function percent(value) { gsub(/%/, "", value); return value + 0 }
	$1 == "total:" { statements = percent($3); next }
	/\.go:/ {
		functions++
		if (percent($NF) > 0) covered++
	}
	END {
		function_percent = functions > 0 ? 100 * covered / functions : 0
		printf "statement coverage: %.1f%% (gate %d%%)\n", statements, min_statements
		printf "functions covered:  %d/%d = %.1f%% (gate %d%%)\n", covered, functions, function_percent, min_functions
		if (statements < min_statements || function_percent < min_functions) exit 1
	}'
