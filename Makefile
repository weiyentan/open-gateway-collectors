.PHONY: build test lint vet clean run docker-build help

APP_NAME    := opencode-collector
BIN_DIR     := bin
BINARY      := $(BIN_DIR)/$(APP_NAME)
GO          := go
GOFLAGS     := -ldflags="-s -w"
CGO_ENABLED := 0

# Default target
help:
	@echo "Usage:"
	@echo "  make build        - Build the binary"
	@echo "  make test         - Run tests"
	@echo "  make lint         - Run golangci-lint (if installed)"
	@echo "  make vet          - Run go vet"
	@echo "  make clean        - Remove build artifacts"
	@echo "  make run          - Build and run the binary"
	@echo "  make docker-build - Build Docker image"

build:
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build $(GOFLAGS) -o $(BINARY) ./cmd/$(APP_NAME)

test:
	$(GO) test -v -count=1 ./...

vet:
	$(GO) vet ./...

lint:
	@which golangci-lint > /dev/null 2>&1 && golangci-lint run ./... || (echo "golangci-lint not installed, skipping"; exit 0)

clean:
	@rm -rf $(BIN_DIR)
	@rm -f *.test
	@rm -rf tmp/

run: build
	@echo "=== Running $(APP_NAME) ==="
	@./$(BINARY)

docker-build:
	docker build -t $(APP_NAME):latest .
