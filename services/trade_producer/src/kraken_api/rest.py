from typing import Dict, List
from loguru import logger
class KrakenRestAPI:
    
    URL = 'https://api.kraken.com/0/public/Trades?pair={product_id}&since={since_sec}'

    def __init__(self, product_ids: List[str],
                from_ms: int,
                to_ms: int) -> None:
        '''
        Basic initialization of the KrakenRestAPI class

        Args:
            product_ids(List[str]): list of product ids for which we want to get trades
            from_ms(int): timestamp in milliseconds from which we want to get trades
            to_ms(int): timestamp in milliseconds up to which we want to get trades
        
        Returns:
            None
        
        '''
        self.product_ids = product_ids
        self.from_ms = from_ms
        self.to_ms = to_ms

        #are we done fetching the historical data?
        # Yes if the last bacth of trades has a data['result'][self.product_ids[0]] >= self.to_ms
        self._is_done = False
    
    def get_trades(self) -> List[Dict]:
        """
        Fetches a batch of trades from Kraken REST API and returns them as a list of dictionaries
        
        Args:
            None

        Returns:
            List[Dict]: list of dictionaries containing trade data
        """
        import requests
        payload = {}
        headers = {'Accept': 'application/json'}
        since_sec = self.from_ms//1000
        url = self.URL.format(product_id=self.product_ids[0], since_sec=since_sec)

        response = requests.request("GET", url, headers=headers, params=payload)

        # print(response.text)

        
        import json
        data=json.loads(response.text)
        
        # TODO: add error handling
        # if data['error'] is not None:
        #     raise Exception(data['error'])
        
        trades = []
        for trade in data['result'][self.product_ids[0]]:
            trades.append(
                {
                    # 'product_id': self.product_ids[0],
                    'price': float(trade[0]),
                    'volume': float(trade[1]),
                    'timestamp': int(trade[2]),
                    'product_id': self.product_ids[0]
                }
            )

        last_ts_in_ns = int(data['result']['last'])
        # convert nanoseconds to milliseconds
        last_ts = last_ts_in_ns // 1_000_000
        if last_ts >= self.to_ms:
            # yes, we are done
            self._is_done = True

        logger.debug(f'got {len(trades)} trades')
        logger.debug(f'last trade timestamp: {last_ts}')


        return trades
        # breakpoint()
    
    def is_done(self) -> bool:
        return self._is_done