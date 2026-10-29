.PHONY: build build-linux test integration

# FSEvents (macOS watcher) needs cgo; the Linux build is static.
build:
	CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o gozeks ./cmd/gozeks

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o dist/gozeks-linux-amd64 ./cmd/gozeks

test:
	go test -race ./...

# needs an S3 endpoint; see test/README.md
integration:
	go test -race -tags integration -count=1 -timeout 10m ./test/...
