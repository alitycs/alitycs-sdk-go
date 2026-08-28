# Releasing

1. Update `Version` in `doc.go` and move user-visible entries from `Unreleased` into a dated
   release section in a pull request.
2. Run `test -z "$(gofmt -l .)"`, `go vet ./...`, `go test -race ./...`,
   `./scripts/check-coverage.sh`, and the governance checks.
3. Merge the release pull request to `main` after CI and CodeRabbit review.
4. Create and push an annotated `vMAJOR.MINOR.PATCH` tag on the merged `main` commit.
5. The release workflow verifies that the annotated tag belongs to reviewed `main`, rebuilds the
   source archive, rechecks tag identity, attests the artifacts, and creates the GitHub Release.

The active `Immutable release tags` ruleset matches `refs/tags/v*`, blocks tag updates and
deletions, and has no bypass actors.

The Go module becomes available through standard module proxies from the immutable GitHub tag.
