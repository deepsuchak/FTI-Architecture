import json
from typing import Dict, List

from loguru import logger

## getting data from Kraken is like getting continuous stream of data from a data source, once you call it you keep on receiving data whereas with rest api you have to call it again and again
from websocket import create_connection


class KrakenWebsocketTradeApi:
    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, product_id: str):
        self.product_id = product_id

        self._ws = create_connection(self.URL)

        logger.info('Connection established')

        # subscribe to trades for given product_id
        self.subscibe(product_id)

    def subscibe(self, product_id: str):
        """
        Establishes connection to the kraken websocketand subscribes to trades for given product_id
        """
        logger.info('Subscribing to trades for {}'.format(product_id))
        ## only creating connection is not enough to get data, you need to subscribe to the channel to get the data
        msg = {
            'method': 'subscribe',
            'params': {'channel': 'trade', 'symbol': [product_id], 'snapshot': False},
        }
        self._ws.send(json.dumps(msg))
        logger.info('Subscription worked!')

        # dumping the first 2 msgs because they contain no train data
        # just confirmation that connection is established
        _ = self._ws.recv()
        _ = self._ws.recv()

    def get_trades(self) -> List[Dict]:
        # mock_trades = [
        #     {'product_id': 'BTC/USD',
        #      'price': 60000,
        #      'volume': 0.01,
        #      'timestamp': 123456789
        #     },
        #      {'product_id': 'BTC/USD',
        #      'price': 59000,
        #      'volume': 0.01,
        #      'timestamp': 15000000
        #     }
        # ]
        message = self._ws.recv()
        if 'heartbeat' in message:
            # when we get heartbeat message we return empty list
            return []

        message = json.loads(message)

        ## extracting data from message['data]
        trades = []
        for trade in message['data']:
            trades.append(
                {
                    'product_id': self.product_id,
                    'price': trade['price'],
                    'volume': trade['qty'],
                    'timestamp': trade['timestamp'],
                }
            )

        # logger.info('Message received: ', message)

        return trades
