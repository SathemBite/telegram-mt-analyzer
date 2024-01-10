from decimal import Decimal
import re
import pandas as pd

class TgTradeData:

    def __init__(self, account_name, algorithm_name, ticker, result_usdt, result_percent, asset_volume, asset_price,
                 asset_price_change, operation):
        self.account_name: str = account_name
        self.algorithm_name: str = algorithm_name
        self.ticker: str = ticker
        self.result_usdt: Decimal = result_usdt
        self.result_percent: Decimal = result_percent
        self.is_profitable: bool = result_usdt > 0
        self.is_loss: bool = self.result_percent < 0
        self.asset_volume: Decimal = asset_volume
        self.asset_price: Decimal = asset_price
        self.asset_price_change_percent: Decimal = asset_price_change
        self.is_long_trade = operation == 'sold'

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(vars(self))

    @classmethod
    def from_tg_trade_data_str(cls, tradeDataStr: str) -> 'TgTradeData':
        print(tradeDataStr)
        pattern = (
            r"^(.*?),\s(.*?)\s*:\s*[⬆⬇]\s*\(F\)\s*(.*?)\s*\$(-?[0-9]+(?:\.[0-9]+)?)\s*\(([+-]?\d*\.?\d+|0.0)%\)\s*"
            r"#(.*?),\s(.*?)\s(\d+(?:\.[0-9]+)?)\sx\s(-?[0-9]+(?:\.[0-9]+)?)\s\(([+-]?\d*\.\d+|0.0)%\)$")

        match = re.match(pattern, tradeDataStr)

        if match:
            account_name = match.group(1)
            algorithm_name = match.group(2)
            result_type = match.group(3)
            result_usdt = Decimal(match.group(4))
            result_percent = Decimal(match.group(5))
            ticker = match.group(6)
            operation = match.group(7)
            asset_volume = Decimal(match.group(8))
            asset_price = Decimal(match.group(9))
            asset_price_change = Decimal(match.group(10))

            if operation != 'sold' and operation != 'bought':
                raise Exception("Строка не соответствует шаблону.")

            return TgTradeData(account_name, algorithm_name, ticker, result_usdt, result_percent, asset_volume, asset_price, asset_price_change, operation)

        else:
            raise Exception("Строка не соответствует шаблону.")
