.PHONY: up down dev test lint

up:
	docker compose up -d --wait

down:
	docker compose down

dev: up

test:
	uv run pytest

lint:
	uv run ruff format --check . && uv run ruff check . && uv run mypy pipeline
