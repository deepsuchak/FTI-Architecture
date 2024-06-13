import os
# kafka_broker_address='redpanda-0:9092' # for docker this is the internal port address from the redpanda.yml file
# kaka_broker_address='localhost:19092', # for running locally (external port address)
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
kafka_broker_address= os.environ.get('KAFKA_BROKER_ADDRESS')
kafka_topic_name='trade'
product_id='BTC/USD'