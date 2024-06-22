import json
from quixstreams import Application
from loguru import logger
from src.hopsworks_api import push_data_to_feature_store
def kafka_to_feature_store(
    kafka_broker_address: str,
    kafka_topic: str,
    feature_group_name: str,
    feature_group_version: int
) -> None:
    """
    Reads trades from redpanda/kafka topic and saves it to the feature store 
    More specifically, it aggregates trades into OHLC candles and sends them to the feature group
    specified by a 'feature group name and version'

    Args:
        kafka_broker_address(str): address of kafka broker
        kafka_topic(str): kafka topic to read trade data from
        feature_group_name(str): name of the feature group to write to
        feature_group_version(int): version of the feature group to write to
    Returns:
        None
    """

    app = Application(
        broker_address=kafka_broker_address,
        consumer_group='kafka_to_feature_store',
        # auto_offset_reset="earliest", # process all msgs from the input topic when this service started
        # auto_create_reset='latest' # forget about past msgs, process only one which come from this moment
    )

    # input_topic = app.topic(name=kafka_topic, value_serializer='json')

    # create a consumer and start a polling loop
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic])

        while True:
            msg = consumer.poll(1)
            
            if msg is None:
                continue
            
            elif msg.error():
                logger.error('kafka error:', msg.error())
                continue

            else:
                # there is data we need to send to feature store

                # step 1: parse the msg from kafka to dictionary
                ohlc = json.loads(msg.value().decode('utf-8')) ## the og msg was in binary so we need to decode

                # step 2: send the data to feature store
                push_data_to_feature_store(feature_group_name=feature_group_name,
                                            feature_group_version=feature_group_version,
                                            data=ohlc)

            
            # we are able read it up to this msg so whenever we are down and we need to spin it up again, start it from there
            # storing offset is a way to tell kafka where we are in the stream (how far)
            consumer.store_offsets(message=msg)


if __name__ == '__main__':
    from src.config import config

    kafka_to_feature_store(kafka_broker_address=config.kafka_broker_address,
                            kafka_topic=config.kafka_topic,
                            feature_group_name=config.feature_group_name,
                            feature_group_version=config.feature_group_version)


