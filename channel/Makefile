.PHONY: build test test-core test-store test-integration test-other lint fmt fmt-check

build:
	go build ./...

test:
	python3 scripts/check_core_test_deps.py
	python3 scripts/run_tests.py all

test-core:
	python3 scripts/check_core_test_deps.py
	python3 scripts/run_tests.py core

test-store:
	python3 scripts/run_tests.py store

test-integration:
	python3 scripts/run_tests.py integration

test-other:
	python3 scripts/run_tests.py other

lint:
	python3 scripts/check_core_test_deps.py
	golangci-lint run ./...

fmt:
	golangci-lint fmt ./...

fmt-check:
	golangci-lint fmt --diff ./...
