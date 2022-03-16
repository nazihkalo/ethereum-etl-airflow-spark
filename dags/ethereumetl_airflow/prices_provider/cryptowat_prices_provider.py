import re
from datetime import datetime
from typing import List, Optional

import requests

from dags.ethereumetl_airflow.prices_provider.prices_provider import PricesProvider, Pair, PriceRecord


class CryptowatPricesProvider(PricesProvider):
    def __init__(self, api_key: str):
        super().__init__(host='https://api.cryptowat.ch', api_key=api_key)

    @staticmethod
    def _get_symbol_from_pair(pair: str) -> Optional[str]:
        match_result = re.findall(r'(.+)(usd|usdc|usdt)$', pair)
        return None if len(match_result) == 0 else match_result[0][0]

    def get_all_usd_pair(self) -> List[Pair]:
        res = requests.get(url=self.host + '/markets')
        usd_pair = []
        symbol_set = set()
        for i in res.json()['result']:
            symbol = self._get_symbol_from_pair(i['pair'])
            if symbol is not None and symbol not in symbol_set:
                usd_pair.append(Pair(pair=i['pair'], market=i['exchange']))
                symbol_set.add(symbol)
        return usd_pair

    def get_single_pair_daily_price(self, pair: Pair, periods: int, start: int, end: int) -> List[PriceRecord]:
        res = requests.get(
            url=self.host + f'/markets/{pair.market}/{pair.pair}/ohlc',
            params={
                'periods': periods,
                'after': start,
                'apikey': self.api_key,
                'before': end
            }
        )

        return [PriceRecord(
            time=datetime.fromtimestamp(i[0]),
            price=i[1],
            symbol=self._get_symbol_from_pair(pair.pair)
        ) for i in res.json()['result'][str(periods)]]
