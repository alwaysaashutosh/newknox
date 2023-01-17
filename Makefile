CURDIR=$(shell pwd)

.PHONY: build
build:
	cd $(CURDIR); GOPRIVATE=github.com/accuknox/; go mod tidy; go build ./...

.PHONY: test
test:
	cd $(CURDIR); go test -v ./...

.PHONY: fmt
fmt:
	cd $(CURDIR); gofmt -s -d $(shell find . -type f -name '*.go' -print)
	cd $(CURDIR); test -z "$(shell gofmt -s -l $(shell find . -type f -name '*.go' -print) | tee /dev/stderr)"

.PHONY: lint
lint:
ifeq (, $(shell which revive))
	@{ \
	set -e ;\
	GOLINT_TMP_DIR=$$(mktemp -d) ;\
	cd $$GOLINT_TMP_DIR ;\
	go mod init tmp ;\
	go install github.com/mgechev/revive@latest ;\
	rm -rf $$GOLINT_TMP_DIR ;\
	}
endif
	cd $(CURDIR); revive ./...

.PHONY: sec
sec:
ifeq (, $(shell which gosec))
	@{ \
	set -e ;\
	GOSEC_TMP_DIR=$$(mktemp -d) ;\
	cd $$GOSEC_TMP_DIR ;\
	go mod init tmp ;\
	go install github.com/securego/gosec/v2/cmd/gosec@latest ;\
	rm -rf $$GOSEC_TMP_DIR ;\
	}
endif
	cd $(CURDIR); gosec ./...
