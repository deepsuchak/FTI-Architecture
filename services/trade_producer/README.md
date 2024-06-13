# The trade producer service

## What does this service do?

This is the first step of our feature pipeline. This Python microservices

- reads trade events from the Kraken Websocket API, and
- saves them into a Kafka topic

## How to run this service?

## Set up
To run this service locally you first need to start locally the message bus (Redpanda in this case).
```
$ cd ../../docker-compose && make start-redpanda
```

### Without Docker

- Create an `.env` file with the KAFKA_BROKER_ADDRESS
    ```
    $ cp .sample.env .env
    # add the info and save it
    ```
    
- `poetry run python src/main.py`

### With Docker

Build the Docker image for this service, and run the container
```
$ make run
```