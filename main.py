import ccxtpro
from asyncio import gather, run, sleep
import os
import aiofiles
import zlib
import base64
import json
import time
from datetime import datetime

from sms import send_sms

def directory_size(path):

    total = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += directory_size(entry.path)
    return total

async def size_monitor(path):

    while True:

        await sleep(42300)

        size = directory_size(path)
        
        send_sms(f'Storage capacity at {size / (1000**3)}g')

def save_data(iso8601, path, method, data):

    try:

        filename = '-'.join([method, iso8601])
        with open(os.path.join(path, '.'.join([filename, 'zlib'])), mode='w') as f:
            bytes = base64.b64encode(
                zlib.compress(
                    json.dumps(data).encode('utf-8')
                )
            ).decode('ascii')
            f.write(bytes)

    except Exception as e:
        print(type(e))
        send_sms(f'Failed writing file')

async def symbol_loop(exchange, method, symbol, path):

    trade_map: dict = {}

    print('Starting', exchange.id, method, symbol)
    while True:

        try:
            response = await getattr(exchange, method)(symbol)
            if method == 'watchOrderBook':
                iso8601 = response.get(symbol).get('datetime')
                save_data(iso8601, path, method, response)
            elif method == 'watchTrades':

                for item in response:

                    if 'info' in item:
                        del item['info']

                    iso8601 = item.get('datetime')
                    if iso8601 not in trade_map:

                        for k, v in trade_map.items():
                            v = sorted(v, key=lambda d: d['id'])
                            save_data(iso8601, path, method, v)

                        trade_map.clear()
                        trade_map[iso8601] = [item]

                        continue

                    trade_map[iso8601].append(item)
             
                exchange.trades[symbol].clear()

        except (ccxtpro.NetworkError, ccxtpro.ExchangeError, Exception) as e:

            print(e)

            # send_sms(f'{iso8601}\n\nSymbol loop error')

            if type(e).__name__ == 'NetworkError':
                raise ccxtpro.NetworkError(e)

async def method_loop(exchange, method, symbols, path):
    print('Starting', exchange.id, method, symbols)
    loops = [symbol_loop(exchange, method, symbol, path) for symbol in symbols]
    await gather(*loops)


async def exchange_loop(exchange_id, methods, path, config={}):

    path = os.path.join(path, exchange_id)
    if not os.path.exists(path):
        os.mkdir(path)

    print('Starting', exchange_id, methods)
    exchange = getattr(ccxtpro, exchange_id)()
    for attr, value in config.items():
        setattr(exchange, attr, value)
    loops = [method_loop(exchange, method, symbols, path) for method, symbols in methods.items()]
    await gather(*loops)
    await exchange.close()

async def main():

    save_path = os.path.join(os.getcwd(), 'data')
    if not os.path.exists(save_path):
        os.mkdir(save_path)

    config = {
        'okx': { 'rateLimit': 10, 'watchOrderBook': { 'depth': 'books' }},
        'bitfinex': { 'rateLimit': 10 },
        'ftx': { 'rateLimit': 10 }
    }

    exchanges = {
        'okx': {
            'watchOrderBook': ['BTC/USDT:USDT'],
            'watchTrades': ['BTC/USDT:USDT'],
        },
        'bitfinex': {
            'watchOrderBook': ['BTC/USD'],
            'watchTrades': ['BTC/USD'],
        },
        'ftx': {
            'watchOrderBook': ['BTC/USD:USD'],
            'watchTrades': ['BTC/USD:USD'],
        },
    }

    loops = [exchange_loop(exchange_id, methods, save_path, config.get(exchange_id, {})) for exchange_id, methods in exchanges.items()]
    loops.append(size_monitor(save_path))
    await gather(*loops)
    
run(main())