import os
import ccxtpro
import lzma
import time
import datetime
import logging

from datetime import datetime
from asyncio import gather, run, sleep
from sms import send_sms

FORMAT= '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT,level=logging.INFO)

DIRECTORY = '/mnt/volume_ams3_01/'

class OrderBook:
    def __init__(self, object: dict):
        self.bids: list = object.get('bids').copy()
        self.asks: list = object.get('asks').copy()
        self.timestamp: int = object.get('timestamp') or int(time.time() * 1000)

    def format(self):

        for i in range(0, len(self.bids)):
            self.bids[i] = '{} {}'.format(self.bids[i][0], self.bids[i][1]) 

        for i in range(0, len(self.asks)):
            self.asks[i] = '{} {}'.format(self.asks[i][0], self.asks[i][1]) 

        return 'timestamp:{};bids:{};asks:{}\n'.format(self.timestamp, ','.join(self.bids), ','.join(self.asks)).encode()

class Trade:
    def __init__(self, object: dict):
        self.id: str = object.get('id')
        self.price: float = object.get('price')
        self.amount: float = object.get('amount')
        self.side: str = object.get('side')
        self.timestamp: int = object.get('timestamp') or int(time.time() * 1000)

    def format(self):
        return 'id:{};price:{};amount:{};side:{};timestamp:{}\n'.format(self.id, self.price, self.amount, self.side, self.timestamp).encode()

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

def save_data(id, path, data):

    try:

        timestamp = data.timestamp
        filename = '.'.join([os.path.join(path, ymd(timestamp)), 'lzma'])

        with lzma.open(filename, 'ab') as f:
            f.write(data.format())
            f.close()

    except Exception as e:
        logging.error('{} - save data - {}'.format(id, str(e)))

        send_sms('{}\n\nProblem saving file'.format(program_time()))
        raise e

async def symbol_loop(exchange, method, symbol, path):

    logging.info('Starting {} {} {}'.format(exchange.id, method, symbol))

    while True:

        try:
            response = await getattr(exchange, method)(symbol)
            if method == 'watchOrderBook':
                save_data(exchange.id, path, OrderBook(response))
            elif method == 'watchTrades':

                for item in response:
                    save_data(exchange.id, path, Trade(item))
                exchange.trades[symbol].clear()

        except (ccxtpro.NetworkError, ccxtpro.ExchangeError, Exception) as e:

            logging.error('{} - symbol loop - {}'.format(exchange.id, str(e)))

            if type(e).__name__ == 'ExchangeError' or type(e).__name__ == 'Exception':

                send_sms('{}\n\nProblem watching {} {}'.format(program_time(), exchange.id, method))

                raise e

            await exchange.sleep(5000)
            

async def method_loop(exchange, method, symbols, path):

    directory = { 'watchOrderBook': 'order_book', 'watchTrades': 'trades' }

    path = os.path.join(path, directory[method])
    if not os.path.exists(path):
        os.mkdir(path)

    loops = [symbol_loop(exchange, method, symbol, path) for symbol in symbols]
    await gather(*loops)


async def exchange_loop(exchange_id, methods, path, config = {}):

    path = os.path.join(path, exchange_id)
    if not os.path.exists(path):
        os.mkdir(path)

    exchange = getattr(ccxtpro, exchange_id)()
    for attr, value in config.items():
        if attr == 'options':
            exchange.options.update(value)
            continue

        setattr(exchange, attr, value)

    loops = [method_loop(exchange, method, symbols, path) for method, symbols in methods.items()]
    await gather(*loops)
    await exchange.close()

async def main():

    save_path = os.path.join(os.getcwd(), DIRECTORY)
    if not os.path.exists(save_path):
        os.mkdir(save_path)

    config = {
        'okx': {
            'options': { 
                'rateLimit': 10, 
                'watchOrderBook': { 'depth': 'books' }
            }
        }
    }

    exchanges = {
        'okx': {
            'watchOrderBook': ['BTC/USDT:USDT'],
            'watchTrades': ['BTC/USDT:USDT'],
        }
    }

    loops = [exchange_loop(exchange_id, methods, save_path, config.get(exchange_id, {})) for exchange_id, methods in exchanges.items()]
    loops.append(watch_storage_space())
    await gather(*loops)
    
run(main())


