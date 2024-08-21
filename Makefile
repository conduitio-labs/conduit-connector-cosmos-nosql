VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-cosmos-nosql.version=${VERSION}'" -o conduit-connector-cosmos-nosql cmd/connector/main.go

.PHONY: test
test:
	go test $(GOTEST_FLAGS) -v -race ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: dep
dep:
	go mod download
	go mod tidy

.PHONY: paramgen
paramgen:
	paramgen -path=./source -output=source_params.go Config
	paramgen -path=./destination -output=destination_params.go Config

.PHONY: mockgen
mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go

.PHONY: generate
generate:
	go generate ./...

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -I % go list -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy
