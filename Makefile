.PHONY: up down dev test lint

# Spark driver needs Java 21. CI sets JAVA_HOME; on a Mac with `brew install openjdk@21` this does.
export JAVA_HOME ?= /opt/homebrew/opt/openjdk@21

up:
	docker compose up -d --wait

down:
	docker compose down

dev: up

test:
	uv run pytest

lint:
	uv run ruff format --check . && uv run ruff check . && uv run mypy pipeline
