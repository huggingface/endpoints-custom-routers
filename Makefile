IMAGE ?= kvrouter
TAG   ?= latest

DIR := queued-least-latency

.PHONY: build build-binary test fmt quality clean

build:
	docker build -t $(IMAGE):$(TAG) ./$(DIR)

build-binary:
	cd $(DIR) && CGO_ENABLED=0 go build -ldflags="-s -w" -o ../kvrouter .

test:
	cd $(DIR) && go test ./...

fmt:
	cd $(DIR) && gofmt -w .

quality:
	cd $(DIR) && go vet ./...

clean:
	rm -f kvrouter
