.DEFAULT_GOAL := simulate

simulate:
	go run ./stdoutinator | go run .
build:
	go build -o aggregator.bin
