import os
import ccxtpro
import time
import datetime
import logging
import json
import glob
import lzma
import argparse
import requests
import aiohttp


from datetime import datetime
from asyncio import gather, run, sleep
from sms import send_sms
from store import Store

FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

DIRECTORY = None

class Exchange(ccxtpro.okx):

    def on_connected(self, client, message = None):
        logging.info('{exchange_id} Connected to {url} - {message}'.format(exchange_id=self.id, url=client.url, message=message))


def program_time():
    try:
        timestamp = int(time.time() * 1000)
        utc = datetime.utcfromtimestamp(timestamp // 1000)
        return utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-6] + "{:03d}".format(int(timestamp) % 1000) + 'Z'
    except (TypeError, OverflowError, OSError):
        return None

def ymd(timestamp):
    utc_datetime = datetime.utcfromtimestamp(int(round(timestamp / 1000)))
    return utc_datetime.strftime('%Y-%m-%d')

def seconds_until_midnight():
    n = datetime.utcnow()
    return ((24 - n.hour - 1) * 60 * 60) + ((60 - n.minute - 1) * 60) + (60 - n.second)

def directory_size(path):

    total = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += directory_size(entry.path)
    return total

async def watch_storage_space():

    while True:
        await sleep(seconds_until_midnight())
        send_sms(f'Storage capacity at {directory_size(DIRECTORY) / (1000**3)}')

async def symbol_loop(exchange, method, symbol: str, path):

    symbol = exchange.market_id(symbol)

    path = os.path.join(path, symbol)
    if not os.path.exists(path):
        os.mkdir(path)

    store = Store(os.path.join(path, 'store.db'))

    logging.info('Starting {} {} {}'.format(exchange.id, method, symbol))

    while True:

        try:
            response = await getattr(exchange, method)(symbol)
            if method == 'watchOrderBook':
                pass
            elif method == 'watchTrades':
                for item in response:

                    side = 'BUY' if item.get('side') == 'buy' else 'SELL'
                    timestamp = int(item.get('timestamp'))

                    store.insert({ 'id': item.get('id'), 'side': side, 'amount': item.get('amount'), 'price': item.get('price'), 'timestamp': timestamp })
                    
                exchange.trades[symbol].clear()

        except (ccxtpro.NetworkError, ccxtpro.ExchangeError, Exception) as e:

            if type(e).__name__ == 'NetworkError':

                logging.warning('{} - symbol loop - {}'.format(exchange.id, str(e)))

            if type(e).__name__ == 'ExchangeError' or type(e).__name__ == 'Exception':

                logging.error('{} - symbol loop - {}'.format(exchange.id, str(e)))
                send_sms('{}\n\nProblem watching {} {}'.format(program_time(), exchange.id, method))

                raise e

async def method_loop(exchange, method, symbols, path):

    directory = { 'watchOrderBook': 'order_book', 'watchTrades': 'trades' }

    path = os.path.join(path, directory[method])
    if not os.path.exists(path):
        os.mkdir(path)

    loops = [symbol_loop(exchange, method, symbol, path) for symbol in symbols]
    await gather(*loops)


async def exchange_loop(exchange_id, methods, path, config={}):

    path = os.path.join(path, exchange_id)
    if not os.path.exists(path):
        os.mkdir(path)

    exchange = Exchange()
    for attr, value in config.items():
        if attr == 'options':
            exchange.options.update(value)
            continue

        setattr(exchange, attr, value)

    await exchange.load_markets(reload = False)

    loops = [method_loop(exchange, method, symbols, path) for method, symbols in methods.items()]
    await gather(*loops)
    await exchange.close()

async def main():

    parser = argparse.ArgumentParser(description = 'exchange data collection service')
    parser.add_argument('-d', type = str, help = 'storage directory', required = True)

    args = parser.parse_args()
    DIRECTORY = getattr(args, 'd', None)

    save_path = os.path.join(os.getcwd(), DIRECTORY)
    if not os.path.exists(save_path):
        os.mkdir(save_path)

    config = {
        'okx': {
            'options': {
                'rateLimit': 10,
                'watchOrderBook': {'depth': 'books'}
            }
        }
    }

    exchanges = {
        'okx': {
            # 'watchOrderBook': ['BTC/USD:BTC', 'BTC/USDT:USDT'],
            'watchTrades': ['BTC/USDT:USDT', 'BTC/USD:BTC'],
        }
    }

    loops = [exchange_loop(exchange_id, methods, save_path, config.get(
        exchange_id, {})) for exchange_id, methods in exchanges.items()]
    loops.append(watch_storage_space())
    await gather(*loops)

run(main())
