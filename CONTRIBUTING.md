# Contributing

Changes to the Alitycs Go SDK must preserve wire compatibility, context cancellation, goroutine
safety, bounded delivery, and honest lifecycle outcomes.

Run these checks before opening a pull request:

```bash
gofmt -w .
go vet ./...
go test -race ./...
./scripts/check-coverage.sh
./scripts/verify-workflow-pins.rb
./scripts/validate-coderabbit.sh
./scripts/test-coderabbit-policy.rb
```

Use private vulnerability reporting for security findings. Never commit credentials, customer
data, build output, or local environment files. Keep `CHANGELOG.md` current for consumer-visible
changes.

CodeRabbit automatically reviews ready pull requests, including dependency updates. Its native
status reports review completion, not approval. Resolve blocking findings and check its formal
review after every push. Governance changes additionally require code-owner approval; see
[CodeRabbit reviews](docs/coderabbit.md).
