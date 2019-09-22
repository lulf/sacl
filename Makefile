all: build 

container_build: build
	podman build -t slim-server:latest .

build: builddir
	GOOS=linux GOARCH=amd64 go build -o build/slim-server cmd/slim-server/main.go
	GOOS=linux GOARCH=amd64 go build -o build/slim-producer cmd/slim-producer/main.go
	GOOS=linux GOARCH=amd64 go build -o build/slim-consumer cmd/slim-consumer/main.go

test:
	go test -v ./...

builddir:
	mkdir -p build

clean:
	rm -rf build
