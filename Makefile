.PHONY: build test lint dep paramgen

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