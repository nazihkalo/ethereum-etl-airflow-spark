import datetime
import json
from typing import List


class Pair:
    def __init__(self, pair: str, market: str):
        self.pair = pair
        self.market = market


class PriceRecord:
    def __init__(self, symbol: str, time: datetime.datetime, price: float):
        self.symbol = symbol
        self.time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.price = price
        self.dt = time.strftime("%Y-%m-%d")


class PricesProvider:
    def __init__(self, host: str, api_key: str):
        self.host = host
        self.api_key = api_key

    def get_all_usd_pair(self) -> List[Pair]:
        raise NotImplementedError()

    def get_single_pair_daily_price(self, pair: Pair, periods: int, start: int, end: int) -> List[PriceRecord]:
        raise NotImplementedError()

    def create_temp_json(self, output_path: str, periods: int, start: int, end: int) -> str:
        with open(output_path, 'w') as f:
            pairs = self.get_all_usd_pair()

            for pair in pairs:
                records = self.get_single_pair_daily_price(pair, periods, start, end)
                f.write(json.dumps([record.__dict__ for record in records]))

        return output_path
