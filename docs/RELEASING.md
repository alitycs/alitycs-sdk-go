# Releasing

1. Move user-visible entries from `Unreleased` into a dated release section in a pull request.
2. Run `go vet ./...`, `./scripts/check-coverage.sh`, and the governance checks.
3. Merge the release pull request to `main` after CI and CodeRabbit review.
4. Create and push an annotated `vMAJOR.MINOR.PATCH` tag on the merged `main` commit.
5. The release workflow verifies that the immutable annotated tag belongs to reviewed `main`,
   rebuilds and attests the source archive, rechecks tag identity, and creates the GitHub Release.

The Go module becomes available through standard module proxies from the immutable GitHub tag.
