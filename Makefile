.PHONY: setup seed build bench test clean
# Start the database and download samples
setup:
	-sudo systemctl stop mongod || true
	chmod +x scripts/setup_db.sh
seed:
	./scripts/setup_db.sh
build:
	cd python && uv run maturin develop --release
bench:
	PYTHONPATH=python .venv/bin/python python/benchmarks/main.py
test:
	cargo test --workspace
clean:
	docker compose down -v
	cargo clean
	rm -rf python/.venv
