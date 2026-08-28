## Summary

Describe the user-visible change and why it is needed.

## Compatibility

- [ ] Go API and minimum-version impact is documented.
- [ ] No wire-contract change, or the coordinated contract change is explained.
- [ ] The SDK still sends only to worker `/events` and keeps credentials out of source.

## Verification

- [ ] `gofmt -w .`
- [ ] `go vet ./...`
- [ ] `go test -race ./...`
- [ ] `./scripts/check-coverage.sh`
- [ ] `./scripts/verify-workflow-pins.rb`
- [ ] `./scripts/validate-coderabbit.sh`
- [ ] `./scripts/test-coderabbit-policy.rb`

## Automated review

- [ ] Native `CodeRabbit` passed for the latest push; formal review state was checked.
- [ ] Blocking findings are resolved and governance changes have code-owner approval.
