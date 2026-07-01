.PHONY: build test clean run

BINARY_NAME=micros3

build:
	go build -o $(BINARY_NAME) cmd/micros3/main.go

test:
	go test -v ./...

clean:
	rm -f $(BINARY_NAME)
	rm -rf data meta staging uploads

run: build
	./$(BINARY_NAME)
