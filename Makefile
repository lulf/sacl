all: build 

container_build: build
	podman build -t sacl-server:latest .

build: builddir
	GOOS=linux GOARCH=amd64 go build -o build/sacl-server cmd/sacl-server/main.go
	GOOS=linux GOARCH=amd64 go build -o build/sacl-producer cmd/sacl-producer/main.go
	GOOS=linux GOARCH=amd64 go build -o build/sacl-consumer cmd/sacl-consumer/main.go

test:
	go test -v ./...

builddir:
	mkdir -p build

clean:
	rm -rf build
