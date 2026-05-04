# Meridian Stream — Makefile
SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help build clean test

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build all Go services
	go build ./...

test: ## Run all tests
	go test -count=1 -race -v ./...

fmt: ## Format Go code
	go fmt ./...

clean: ## Clean build artifacts
	rm -rf bin/
