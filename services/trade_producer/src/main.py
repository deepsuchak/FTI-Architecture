# # this thing gets msgs from a websocket and sends them to redpanda topic
# from time import sleep
# from typing import Dict, List
# from loguru import logger
# from quixstreams import Application

# from src.kraken_api.websocket import KrakenWebsocketTradeApi
# #from src import config
# from src.config import config

# def produce_trades(kafka_broker_address: str,
#                     kafka_topic_name: str,
#                     product_ids: List[str],
#                     live_or_historical: str,
#                     last_n_days: int) -> None:
#     """
#     Reads trades from Kraken websocket and sends them to redpanda topic

#     Args:
#         kafka_broker_address(str): address of kafka broker
#         kafka_topic_name(str): name of kafka topic
#         product_id(str): name of product id to get trades from Kraken websocket api
#         live_or_historical(str): live or historical trades
#         last_n_days(int): laast n number of days from which we want the historical trades
#     Returns:
#         None
#     """

#     assert live_or_historical in {'live', 'historical'}, f"invalid value for live_or_historical: {live_or_historical}"

#     app = Application(broker_address=kafka_broker_address)

#     # the topic where we will save the trades
#     topic = app.topic(name=kafka_topic_name, value_serializer='json')
#     logger.info(f'Creating the Kraken API to fetch data for {product_ids}...')

#     # event = {"id": 1, "text": "hello world"} '''used for testing'''


#     if live_or_historical == 'live':
#         kraken_api = KrakenWebsocketTradeApi(product_ids=product_ids)
#     else:
#         from src.kraken_api.rest import KrakenRestAPI
#         import time
#         to_ms = int(time.time() * 1000) 
#         from_ms = to_ms - (last_n_days * 24 * 60 * 60 * 1000)

#         kraken_api = KrakenRestAPI(product_ids=product_ids,
#                                     from_ms=from_ms,
#                                     to_ms=to_ms)
    
    
#     logger.info('Creating producer...')

#     with app.get_producer() as producer:
#         while True:
#             ## (we need to check if we are done fetching the historical data)
            
#             if kraken_api.is_done():
#                 logger.info('done fetching historical data')
#                 break
#             # get trades from Kraken
#             trades: List[Dict] = kraken_api.get_trades()
#             # logger.info('got trades from kraken websocket')
#             for trade in trades:
#                 message = topic.serialize(key=trade['product_id'], value=trade)

#                 # key = str(trade["product_id"]).encode('utf-8')
#                 # value = json.dumps(trade).encode('utf-8')

#                 producer.produce(topic=topic.name, value=message.value, key=message.key)
#                 # producer.produce(topic=topic.name, value=message.value, key=message.key)

#                 logger.info(trade)
#                 sleep(1)


# if __name__ == '__main__':
#     produce_trades(
#         kafka_broker_address=config.kafka_broker_address, # for docker this is the internal port address from the redpanda.yml file
#         # kaka_broker_address='localhost:19092', # for running locally
#         kafka_topic_name=config.kafka_topic_name,
#         product_ids=config.product_ids,
        
#         # extra parameters when running the trade_producer against historical data 
#         live_or_historical=config.live_or_historical,
#         last_n_days=config.last_n_days
#     )
from time import sleep
from typing import Dict, List
from loguru import logger
from quixstreams import Application

from src.kraken_api.websocket import KrakenWebsocketTradeApi
from src.config import config

def produce_trades(kafka_broker_address: str,
                    kafka_topic_name: str,
                    product_ids: List[str],
                    live_or_historical: str,
                    last_n_days: int) -> None:
    """
    Reads trades from Kraken websocket and sends them to redpanda topic

    Args:
        kafka_broker_address(str): address of kafka broker
        kafka_topic_name(str): name of kafka topic
        product_id(str): name of product id to get trades from Kraken websocket api
        live_or_historical(str): live or historical trades
        last_n_days(int): laast n number of days from which we want the historical trades
    Returns:
        None
    """

    assert live_or_historical in {'live', 'historical'}, f"invalid value for live_or_historical: {live_or_historical}"

    app = Application(broker_address=kafka_broker_address)

    # the topic where we will save the trades
    topic = app.topic(name=kafka_topic_name, value_serializer='json')
    logger.info(f'Creating the Kraken API to fetch data for {product_ids}...')

    if live_or_historical == 'live':
        kraken_api = KrakenWebsocketTradeApi(product_ids=product_ids)
    else:
        from src.kraken_api.rest import KrakenRestAPI
        import time
        to_ms = int(time.time() * 1000) 
        from_ms = to_ms - (last_n_days * 24 * 60 * 60 * 1000)

        kraken_api = KrakenRestAPI(product_ids=product_ids,
                                    from_ms=from_ms,
                                    to_ms=to_ms)
    
    logger.info('Creating producer...')

    with app.get_producer() as producer:
        while True:
            if kraken_api.is_done():
                logger.info('done fetching historical data')
                break
            
            # get trades from Kraken
            trades = kraken_api.get_trades()
            
            if trades is None:
                logger.error('get_trades returned None')
                break

            if not isinstance(trades, list):
                logger.error(f'get_trades returned an unexpected type: {type(trades)}')
                break

            for trade in trades:
                message = topic.serialize(key=trade['product_id'], value=trade)

                producer.produce(topic=topic.name, value=message.value, key=message.key)

                logger.info(trade)
            sleep(1)

if __name__ == '__main__':
    produce_trades(
        kafka_broker_address=config.kafka_broker_address, # for docker this is the internal port address from the redpanda.yml file
        # kafka_broker_address='localhost:19092', # for running locally
        kafka_topic_name=config.kafka_topic_name,
        product_ids=config.product_ids,
        
        # extra parameters when running the trade_producer against historical data 
        live_or_historical=config.live_or_historical,
        last_n_days=config.last_n_days
    )

    
