.PHONY: build test lint dep paramgen mockgen

build:
	go build -o conduit-connector-cosmos-nosql cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -v -race ./...

lint:
	golangci-lint run --config .golangci.yml

dep:
	go mod download
	go mod tidy

paramgen:
	paramgen -path=./source -output=source_params.go Config
	paramgen -path=./destination -output=destination_params.go Config

mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go