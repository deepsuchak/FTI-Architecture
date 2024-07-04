run-dev:
	poetry run python src/main.py

build:
	docker build -t trade_producer .

run: build
	docker run \
	 --network=redpanda_network \
	 --env KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
	  trade_producer

##confirm with pau
lint:
	poetry run ruff check --fix

format:
	poetry run ruff format .