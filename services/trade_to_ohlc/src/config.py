import os

# kafka_broker_address='redpanda-0:9092' # for docker this is the internal port address from the redpanda.yml file
# kaka_broker_address='localhost:19092', # for running locally (external port address)
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

from pydantic_settings import BaseSettings


class Config(BaseSettings):
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    ohlc_window_seconds: int = os.environ['OHLC_WINDOW_SECONDS']
    kafka_input_topic_name: str = 'trade'
    kafka_output_topic_name: str = 'ohlc'


config = Config()
