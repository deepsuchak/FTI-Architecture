# this part doesnt work, we need to encode the message and the key to utf-8

#     with app.get_producer() as producer:
#         while True:
#             message = topic.serialize(key = event["id"], value = event)

#             producer.produce(topic = topic.name, value = message.value, key = message.key)

#             print('Message sent!')
#             sleep(1)

# this thing gets msgs from a websocket and sends them to redpanda topic
from quixstreams import Application
from time import sleep
import json
from src.kraken_api import KrakenWebsocketTradeApi


def produce_trades(
        kaka_broker_address: str,
        kaka_topic_name: str
) -> None:
    '''
    Reads trades from Kraken websocket and sends them to redpanda topic
    
    Args:
        kaka_broker_address(str): address of kafka broker
        kaka_topic_name(str): name of kafka topic

    Returns:
        None
    '''

    app = Application(broker_address=kaka_broker_address)
    
    # the topic where we will save the trades
    topic = app.topic(name=kaka_topic_name, value_serializer='json')

    # event = {"id": 1, "text": "hello world"} '''used for testing'''
    kraken_api = KrakenWebsocketTradeApi(product_id='BTC/USD')

    with app.get_producer() as producer:
        while True:

            # get trades from Kraken
            trades = kraken_api.get_trades()
    
            for trade in trades:
                
                message = topic.serialize(key = trade["product_id"], value = trade)

                # key = str(trade["product_id"]).encode('utf-8')
                # value = json.dumps(trade).encode('utf-8')

                producer.produce(topic=topic.name, value=message.value, key=message.key)
                # producer.produce(topic=topic.name, value=message.value, key=message.key)

                print('Message sent!')
            sleep(1)


if __name__ == "__main__":
    produce_trades(
        kaka_broker_address='localhost:19092',
        kaka_topic_name='trade'
    )

# from quixstreams import Application
# from time import sleep
# import json
# from src.kraken_api import KrakenWebsocketTradeApi


# def produce_trades(
#         kaka_broker_address: str,
#         kaka_topic_name: str
# ) -> None:
#     '''
#     Reads trades from Kraken websocket and sends them to Redpanda topic
    
#     Args:
#         kaka_broker_address(str): address of Kafka broker
#         kaka_topic_name(str): name of Kafka topic

#     Returns:
#         None
#     '''

#     app = Application(broker_address=kaka_broker_address)
    
#     # the topic where we will save the trades
#     topic = app.topic(name=kaka_topic_name, value_serializer='json')

#     kraken_api = KrakenWebsocketTradeApi(product_id='BTC/USD')

#     with app.get_producer() as producer:
#         while True:
#             # get trades from Kraken
#             trades = kraken_api.get_trades()

#             for trade in trades:
#                 message = topic.serialize(key=trade["product_id"], value=trade)

#                 producer.produce(topic=topic.name, value=message.value, key=message.key)

#                 print('Message sent!')
#             sleep(1)


# if __name__ == "__main__":
#     produce_trades(
#         kaka_broker_address='localhost:19092',
#         kaka_topic_name='trade'
#     )
