all: test

test:
	go test -v ./...

builddir:
	mkdir -p build

clean:
	rm -rf build
