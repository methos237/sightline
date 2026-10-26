.PHONY: up down test lint

# Spark driver needs Java 21. CI sets JAVA_HOME; locally Homebrew's openjdk@21 is the default.
export JAVA_HOME ?= $(shell brew --prefix openjdk@21 2>/dev/null)

up:
	docker compose up -d --wait

down:
	docker compose down

test:
	uv run pytest

lint:
	uv run ruff format --check . && uv run ruff check . && uv run mypy pipeline
