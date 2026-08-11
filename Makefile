.PHONY: lint fmt vet build setup-hooks

GOLANGCI_LINT_VERSION := v1.64.8

lint:
	@which golangci-lint > /dev/null 2>&1 || { \
		echo "Installing golangci-lint $(GOLANGCI_LINT_VERSION)..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); \
	}
	golangci-lint run ./...

fmt:
	gofmt -l -w .
	goimports -l -w -local github.com/volcengine/ark-runtime-go .

vet:
	go vet ./...

build:
	go build ./...

check: build vet lint

setup-hooks:
	@mkdir -p .githooks
	@cp scripts/pre-commit .githooks/pre-commit
	@chmod +x .githooks/pre-commit
	@git config core.hooksPath .githooks
	@echo "Git hooks installed to .githooks/"
