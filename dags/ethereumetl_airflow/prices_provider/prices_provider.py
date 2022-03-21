import copy
import csv
from datetime import datetime
from typing import List

import pendulum as pdl
from coinpaprika import client as Coinpaprika
from ethereumetl.progress_logger import ProgressLogger

from ethereumetl_airflow.prices_provider.tokens_provider.token_provider import (
    TokenProvider,
    Token,
    DuneTokenProvider
)

iso_format = '%Y-%m-%dT%H:%M:%SZ'
minutes_format = '%Y-%m-%d %H:%M'
day_format = '%Y-%m-%d'


class PriceRecord:
    attrs = ['minutes', 'prices', 'decimals', 'contract_address', 'symbol', 'dt']

    def __init__(self,
                 minutes: str,
                 prices: float,
                 decimals: int,
                 contract_address: str,
                 symbol: str,
                 dt: str) -> None:
        self.minutes = minutes
        self.prices = prices
        self.decimals = decimals
        self.contract_address = contract_address
        self.symbol = symbol
        self.dt = dt

    def copy_it_with_datetime(self, time: pdl.datetime) -> 'PriceRecord':
        other = copy.copy(self)
        other.minutes = time.strftime(minutes_format)
        other.dt = time.strftime(day_format)
        return other


class PriceProvider:
    def __init__(self, token_provider: TokenProvider):
        self.token_provider = token_provider
        self.progress_logger = ProgressLogger()

    def get_single_token_daily_price(self, token: Token, start: int, end: int) -> List[PriceRecord]:
        raise NotImplementedError()

    def create_temp_csv(self, output_path: str, start: int, end: int) -> None:
        tokens = self.token_provider.get_tokens()
        self.progress_logger.start(total_items=len(tokens))

        with open(output_path, 'w') as csvfile:
            spam_writer = csv.DictWriter(csvfile, fieldnames=PriceRecord.attrs)
            spam_writer.writeheader()

            for token in tokens:
                if token.end is not None:
                    end_at = int(datetime.strptime(date_string=token.end, format=iso_format).timestamp())
                    if end_at < end:
                        continue

                spam_writer.writerows([i.__dict__ for i in self.get_single_token_daily_price(token, start, end)])
                self.progress_logger.track()

        self.progress_logger.finish()


class CoinpaprikaPriceProvider(PriceProvider):
    def __init__(self):
        super().__init__(token_provider=DuneTokenProvider())

    @staticmethod
    def _copy_record_across_interval(record: PriceRecord, interval: int) -> List[PriceRecord]:
        start = pdl.from_format(record.minutes, "YYYY-MM-DD HH:mm")
        end = start.add(minutes=interval)
        records = []

        while start < end:
            records.append(record.copy_it_with_datetime(start))
            start = start.add(minutes=1)

        return records

    def get_single_token_daily_price(self, token: Token, start: int, end: int) -> List[PriceRecord]:
        client = Coinpaprika.Client()
        records = []
        res = client.historical(coin_id=token.id, start=start, end=end, limit=5000)

        for item in res:
            time = pdl.instance(datetime.strptime(item['timestamp'], '%Y-%m-%dT%H:%M:%SZ'))
            records += self._copy_record_across_interval(
                record=PriceRecord(
                    minutes=time.strftime('%Y-%m-%d %H:%M'),
                    prices=item['price'],
                    decimals=token.decimals,
                    contract_address=token.address,
                    symbol=token.symbol,
                    dt=time.strftime('%Y-%m-%d')
                ),
                interval=5
            )

        return res
