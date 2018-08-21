GO ?= go


CMDS := $(shell go list ./... | grep -v /vendor/ | grep /cmd/)

.PHONY: all build deps 

all: build

build: deps
	@for c in $(CMDS); do \
		echo "==> Building cmd $$c ..."; \
		$(GO) build $$c || exit 1; \
	done

deps: 
	@$(GO) get -d -v ./...