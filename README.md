# Alitycs Go SDK

Official server-side Go SDK for sending analytics events to Alitycs. It supports concurrent
tracking, bounded batching, retry, session and anonymous identity, revenue events, and observable
flush and shutdown outcomes. Go 1.22 or newer is required.

## Install

```bash
go get github.com/alitycs/alitycs-sdk-go@latest
```

## Usage

```go
client, err := alitycs.New("pk_live_...")
if err != nil {
    return err
}
defer client.Shutdown(context.Background())

client.Track(ctx, "signup_completed", alitycs.Props{"plan": "pro"})
client.Identify(ctx, "usr_1842", alitycs.Props{"company": "Acme"})
if err := client.Flush(ctx); err != nil {
    return err
}
```

The SDK sends batches to `https://api.alitycs.com/events` using
`Authorization: Bearer <publishable-key>`. Keep secret keys out of distributed applications.

See the package documentation for configuration, delivery guarantees, event limits, and
request-scoped identity. Releases and checksums are published on
[GitHub](https://github.com/alitycs/alitycs-sdk-go/releases).

## Development

```bash
gofmt -w .
go vet ./...
./scripts/check-coverage.sh
```

The project is licensed under the [MIT License](LICENSE). See [Contributing](CONTRIBUTING.md),
[Security](SECURITY.md), and [Releasing](docs/RELEASING.md).
