build:
	GOARCH=amd64 go build -mod=vendor -a -installsuffix cgo -ldflags '-w' -o bin/crawler cmd/main.go
.PHONY: build

clean:
	make clean
.PHONY: clean

release: build
	docker build -t hub.tess.io/bozhang/go-crawler
.PHONY: release

push: release
	docker push hub.tess.io/bozhang/go-crawler
.PHONY: push
