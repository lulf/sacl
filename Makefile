all: build 

container_build: build
	podman build -t sacl:latest .

build: builddir
	#CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o build/sacl cmd/sacl/main.go
	GOOS=linux GOARCH=amd64 go build -o build/sacl cmd/sacl/main.go
	GOOS=linux GOARCH=amd64 go build -o build/event-generator cmd/event-generator/main.go

test:
	go test -v ./...

builddir:
	mkdir -p build

clean:
	rm -rf build
