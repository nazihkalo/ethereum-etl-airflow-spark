import csv
import datetime
from typing import List

from ethereumetl.progress_logger import ProgressLogger


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
        self.progress_logger = ProgressLogger()

    def get_all_usd_pair(self) -> List[Pair]:
        raise NotImplementedError()

    def get_single_pair_daily_price(self, pair: Pair, periods: int, start: int, end: int) -> List[PriceRecord]:
        raise NotImplementedError()

    def create_temp_json(self, output_path: str, periods: int, start: int, end: int) -> str:
        pairs = self.get_all_usd_pair()
        self.progress_logger.start(total_items=len(pairs))

        with open(output_path, 'w') as csvfile:
            spam_writer = csv.DictWriter(csvfile, fieldnames=['symbol', 'time', 'price', 'dt'])
            spam_writer.writeheader()

            for pair in pairs:
                for record in self.get_single_pair_daily_price(pair, periods, start, end):
                    spam_writer.writerow(record.__dict__)
                self.progress_logger.track()

        self.progress_logger.finish()
        return output_path
