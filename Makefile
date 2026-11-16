.PHONY: all build proto test up down logs demo mock

all: build test

proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/proto/dispatcher/v1/dispatcher.proto

build:
	go build -v -o bin/server ./cmd/server
	go build -v -o bin/mockreceiver ./cmd/mockreceiver
	go build -v -o bin/demo ./cmd/demo

test:
	go test -v ./...

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f

mock:
	go run ./cmd/mockreceiver -port 9090 -secret mock-secret-key-12345 -fail-rate 0.5

demo:
	go run ./cmd/demo -grpc-addr localhost:50051 -http-addr http://localhost:8080 -mock-addr http://localhost:9090 -scenario all
