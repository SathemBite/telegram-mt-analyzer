import time

import sys
from datetime import datetime, timedelta, timezone

import telethon.helpers
from telethon import TelegramClient
from telethon import types
from typing import Optional
import configparser
from TgTradeData import TgTradeData
from telethon.types import MessageService
import pandas as pd


# trade_result_msg_words = ["Loss", "Profit"]


def parse_conf(conf_name: str):
    conf_parser = configparser.ConfigParser()
    conf_parser.read(conf_name)
    return conf_parser


# noinspection PyTypeChecker
def telegram_client(api_id: str, api_hash: str) -> TelegramClient:
    client = TelegramClient('bot', conf['default']['api_id'], conf['default']['api_hash']).start()
    return client


conf_file = sys.argv[1] if len(sys.argv) == 2 else 'config.ini'
conf = parse_conf(conf_file)
tg_client = telegram_client(conf['default']['api_id'], conf['default']['api_hash'])


async def analyze(name):
    # def filter_trades_result_msgs(msg: str):

    mt_stats_channel_id = int(conf['default']['mt_stats_channel_id'])
    mt_stats_channel: types.Channel = await tg_client.get_entity(mt_stats_channel_id)

    offset = 0
    message_limit = 100
    mt_msg_start_text = conf['default']['mt_msg_start_text']
    stats_len = 0
    trade_data_list: list = []

    today: datetime = datetime.utcnow().replace(tzinfo=timezone.utc)

    while True:
        mt_messages: telethon.helpers.TotalList = await tg_client.get_messages(
            mt_stats_channel,
            offset_id=offset,
            limit=message_limit,
            reverse=True
        )

        offset += message_limit

        if len(mt_messages) == 0:
            print('No messages found. The program is stopped')
            break

        def additional_check(msg: types.MessageService):
            ten_days_ago = today - timedelta(days=+50)
            return True if (msg.date > ten_days_ago) else False

        start_time = time.perf_counter()

        for message in mt_messages:
            msg_datetime = message.date
            msg_text = message.text

            if msg_text is not None and msg_text.startswith(mt_msg_start_text) and additional_check(message):
                trades_text = msg_text.split('\n')
                non_empty_trades_text = list(
                    filter(lambda text: text != '' and ('Profit' in text or 'Loss' in text), trades_text))

                for trade_text in non_empty_trades_text:
                    trade = TgTradeData.from_tg_trade_data_str(trade_text)
                    if ('1.5' in trade.algorithm_name or '1.8' in trade.algorithm_name or '2.0'):
                        trade_data_list.append(trade)
                    print(trade_text)
                    stats_len += 1

        end_time = time.perf_counter()

        print(f'Total time: {end_time - start_time}')

    trades_df = pd.DataFrame([vars(trade) for trade in trade_data_list])

    # agg_result = trades_df.groupby(['algorithm_name', 'ticker']).agg({
    #     'result_usdt': 'sum',
    #     'ticker': 'count',
    #     'result_percent': 'mean',
    #     'is_profitable': 'sum',
    #     'is_loss': 'sum'
    # })

    agg_result = trades_df.groupby('ticker').agg({
        'result_usdt': 'sum',
        'ticker': 'count',
        'result_percent': 'mean',
        'is_profitable': 'sum',
        'is_loss': 'sum'
    })

    agg_result = agg_result.rename(columns={'result_usdt': 'total_profit', 'ticker': 'total_trades', 'result_percent': 'average_profit_percent'})

    agg_result['weight'] = abs(agg_result['total_trades'] * agg_result['average_profit_percent'])

    print('Stats len: {}'.format(agg_result))

    time.sleep(1000000)


if __name__ == '__main__':
    with(tg_client):
        tg_client.loop.run_until_complete(analyze('Anton'))

