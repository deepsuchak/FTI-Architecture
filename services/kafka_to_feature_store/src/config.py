import os

# kafka_broker_address='redpanda-0:9092' # for docker this is the internal port address from the redpanda.yml file
# kaka_broker_address='localhost:19092', # for running locally (external port address)
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

from pydantic_settings import BaseSettings


class Config(BaseSettings):
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    feature_group_version: int = 1
    feature_group_name: str = 'ohlc_feature_group'
    kafka_topic: str = 'ohlc'
    hopsworks_project_name: str = os.environ['HOPSWORKS_PROJECT_NAME']
    hopsworks_api_key: str = os.environ['HOPSWORKS_API_KEY']

config = Config()
