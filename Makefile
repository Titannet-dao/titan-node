SHELL=/usr/bin/env bash

all: build
.PHONY: all

unexport GOFLAGS

GOCC?=go

GOVERSION:=$(shell $(GOCC) version | tr ' ' '\n' | grep go1 | sed 's/^go//' | awk -F. '{printf "%d%03d%03d", $$1, $$2, $$3}')
ifeq ($(shell expr $(GOVERSION) \< 1017001), 1)
$(warning Your Golang version is go$(shell expr $(GOVERSION) / 1000000).$(shell expr $(GOVERSION) % 1000000 / 1000).$(shell expr $(GOVERSION) % 1000))
$(error Update Golang to version to at least 1.18)
endif

# git modules that need to be loaded
MODULES:=

CLEAN:=
BINS:=

ldflags=-X=github.com/Filecoin-Titan/titan/build.CurrentCommit=+git.$(subst -,.,$(shell git describe --always --match=NeVeRmAtCh --dirty 2>/dev/null || git rev-parse --short HEAD 2>/dev/null))
ifneq ($(strip $(LDFLAGS)),)
	ldflags+=-extldflags=$(LDFLAGS)
endif

GOFLAGS+=-ldflags="$(ldflags)"


titan-scheduler: $(BUILD_DEPS)
	rm -f titan-scheduler
	$(GOCC) build $(GOFLAGS) -o titan-scheduler ./cmd/titan-scheduler
.PHONY: titan-scheduler

titan-candidate: $(BUILD_DEPS)
	rm -f titan-candidate
	$(GOCC) build $(GOFLAGS) -o titan-candidate ./cmd/titan-candidate
.PHONY: titan-candidate

titan-edge: $(BUILD_DEPS)
	rm -f titan-edge
	$(GOCC) build $(GOFLAGS) -o titan-edge ./cmd/titan-edge
.PHONY: titan-edge

titan-locator: $(BUILD_DEPS)
	rm -f titan-locator
	$(GOCC) build $(GOFLAGS) -o titan-locator ./cmd/titan-locator
.PHONY: titan-locator


build-cross: $(BUILD_DEPS)
	rm -f build/cross/
	gox -gocmd=$(GOCC) $(GOFLAGS) -osarch="darwin/amd64 darwin/arm64 linux/amd64 linux/arm linux/arm64 windows/amd64" -output="build/cross/{{.OS}}/{{.Arch}}/titan-edge" ./cmd/titan-edge
	gox -gocmd=$(GOCC) $(GOFLAGS) -osarch="darwin/amd64 darwin/arm64 linux/amd64 linux/arm linux/arm64 windows/amd64" -output="build/cross/{{.OS}}/{{.Arch}}/titan-candidate" ./cmd/titan-candidate
	gox -gocmd=$(GOCC) $(GOFLAGS) -osarch="darwin/amd64 darwin/arm64 linux/amd64 linux/arm linux/arm64 windows/amd64" -output="build/cross/{{.OS}}/{{.Arch}}/titan-scheduler" ./cmd/titan-scheduler
	gox -gocmd=$(GOCC) $(GOFLAGS) -osarch="darwin/amd64 darwin/arm64 linux/amd64 linux/arm linux/arm64 windows/amd64" -output="build/cross/{{.OS}}/{{.Arch}}/titan-locator" ./cmd/titan-locator
.PHONY: build-cross


api-gen:
	$(GOCC) run ./gen/api
	goimports -w api
.PHONY: api-gen

cfgdoc-gen:
	$(GOCC) run ./node/config/cfgdocgen > ./node/config/doc_gen.go

build: titan-scheduler titan-candidate titan-edge titan-locator
.PHONY: build

install: install-scheduler install-locator install-candidate install-edge

install-scheduler:
	install -C ./titan-scheduler /usr/local/bin/titan-scheduler

install-locator:
	install -C ./titan-locator /usr/local/bin/titan-locator

install-candidate:
	install -C ./titan-candidate /usr/local/bin/titan-candidate

install-edge:
	install -C ./titan-edge /usr/local/bin/titan-edge


edge-image:
	docker build -t edge:latest -f ./cmd/titan-edge/Dockerfile .
.PHONY: edge-image

candidate-image:
	docker build -t candidate:latest -f ./cmd/titan-candidate/Dockerfile .
.PHONY: candidate-image

scheduler-image:
	docker build -t scheduler:latest -f ./cmd/titan-scheduler/Dockerfile .
.PHONY: scheduler-image

locator-image:
	docker build -t locator:latest -f ./cmd/titan-locator/Dockerfile .
.PHONY: locator-image
