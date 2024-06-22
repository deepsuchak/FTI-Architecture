# this thing gets msgs from a websocket and sends them to redpanda topic
from time import sleep

from loguru import logger
from quixstreams import Application

from src.kraken_api import KrakenWebsocketTradeApi
#from src import config
from src.config import config

def produce_trades(kafka_broker_address: str,
                    kafka_topic_name: str,
                    product_id: str) -> None:
    """
    Reads trades from Kraken websocket and sends them to redpanda topic

    Args:
        kafka_broker_address(str): address of kafka broker
        kafka_topic_name(str): name of kafka topic
        product_id(str): name of product id to get trades from Kraken websocket api

    Returns:
        None
    """

    app = Application(broker_address=kafka_broker_address)

    # the topic where we will save the trades
    topic = app.topic(name=kafka_topic_name, value_serializer='json')

    # event = {"id": 1, "text": "hello world"} '''used for testing'''
    kraken_api = KrakenWebsocketTradeApi(product_id=product_id)

    logger.info('Creating producer...')

    with app.get_producer() as producer:
        while True:
            # get trades from Kraken
            trades = kraken_api.get_trades()
            # logger.info('got trades from kraken websocket')
            for trade in trades:
                message = topic.serialize(key=trade['product_id'], value=trade)

                # key = str(trade["product_id"]).encode('utf-8')
                # value = json.dumps(trade).encode('utf-8')

                producer.produce(topic=topic.name, value=message.value, key=message.key)
                # producer.produce(topic=topic.name, value=message.value, key=message.key)

                logger.info(trade)
            sleep(1)


if __name__ == '__main__':
    produce_trades(
        kafka_broker_address=config.kafka_broker_address, # for docker this is the internal port address from the redpanda.yml file
        # kaka_broker_address='localhost:19092', # for running locally
        kafka_topic_name=config.kafka_topic_name,
        product_id=config.product_id
    )
