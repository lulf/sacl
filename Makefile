all: build 

container_build: build
	podman build -t event-store:latest .

build: builddir
	#CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o build/event-store cmd/event-store/main.go
	GOOS=linux GOARCH=amd64 go build -o build/event-store cmd/event-store/main.go

test:
	go test -v ./...

builddir:
	mkdir -p build

clean:
	rm -rf build
