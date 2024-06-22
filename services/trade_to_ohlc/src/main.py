from loguru import logger

def trade_to_ohlc(
    kafka_broker_address: str,
    kafka_input_topic: str,
    kafka_output_topic: str,
    ohlc_window_seconds: int
) -> None:
    '''
    Reads trades from redpanda topic
    Aggregates them into OHLC candles using the window size in 'ohlc_window_seconds'
    Sends aggregated candles to output redpanda topic

    Args:
        kafka_broker_address(str): address of kafka broker
        kafak_input_topic(str): kafka topic to read trade data from
        kafak_output_topic(str): kafka topic to write aggregated candles to
        ohlc_window_seconds(int): window size in seconds for OHLC aggregation 

    Returns:
        None     
    '''
    from quixstreams import Application

    # to handle all low-level communication with redpanda
    app = Application(broker_address=kafka_broker_address,
                      consumer_group="trad_to_ohlc",
                    #   auto_offset_reset="earliest", # process all msgs from the input topic when this service started
                    #   auto_create_reset='latest' # forget about past msgs, process only one which come from this moment
                      )

    input_topic = app.topic(name=kafka_input_topic, value_serializer='json')
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')
    
    ## create a streaming dataframe as per quixstream's docs
    sdf = app.dataframe(input_topic)

    def init_ohlc_candle(value: dict) -> dict:
        '''
        Initializes OHLC candle
        '''
        return {
            'open': value['price'],
            'high': value['price'],
            'low': value['price'],
            'close': value['price'],
            'product_id': value['product_id'],
            
            # 'timestamp': value['timestamp'],    
        }
        

    def update_ohlc_candle(ohlc_candle: dict, trade: dict) -> dict:
        '''
        Updates OHLC candle
        '''
        return {
        "open" : ohlc_candle['open'],
        "high" :  max(ohlc_candle['high'], trade['price']),
        "low" : min(ohlc_candle['low'], trade['price']),
        "close" : trade['price'],
        "product_id": trade['product_id'],
        } 
        


    ## apply transformations -- start
    # TODO
    from datetime import timedelta
    sdf = sdf.tumbling_window(duration_ms=timedelta(seconds=ohlc_window_seconds))
    sdf = sdf.reduce(reducer = update_ohlc_candle, initializer = init_ohlc_candle).final() #current()

    ## apply transformations -- end

    sdf['open'] = sdf['value']['open']
    sdf['high'] = sdf['value']['high']
    sdf['low'] = sdf['value']['low']
    sdf['close'] = sdf['value']['close']
    sdf['product_id'] = sdf['value']['product_id']
    sdf['timestamp'] = sdf['end']

    sdf = sdf[['open', 'high', 'low', 'close', 'product_id', 'timestamp']] 

    ## send aggregated candles to output redpanda topic
    sdf =  sdf.update(logger.info)
    sdf = sdf.to_topic(output_topic)

    #kickoff the streaming dataframe
    app.run(sdf)

if __name__ == '__main__':
    from src.config import config
    trade_to_ohlc(
        kafka_broker_address=config.kafka_broker_address,
        kafka_input_topic=config.kafka_input_topic_name,
        kafka_output_topic=config.kafka_output_topic_name,
        ohlc_window_seconds=config.ohlc_window_seconds 
    ) 