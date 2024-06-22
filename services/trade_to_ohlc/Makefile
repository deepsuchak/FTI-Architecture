build:
	docker build -t trade-to-ohlc .

run: build
	docker run \
		--network=redpanda_network \
		--env KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
		--env KAFKA_INPUT_TOPIC=trade \
		--env KAFKA_OUTPUT_TOPIC=ohlc \
		--env KAFKA_CONSUMER_GROUP=trade_to_ohlc_consumer_group \
		--env OHLC_WINDOW_SECONDS=60 \
		trade-to-ohlc

lint:
	poetry run ruff check --fix

format:
	poetry run ruff format .

lint-and-format: lint format