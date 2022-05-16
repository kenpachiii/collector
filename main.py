import ccxtpro
from asyncio import gather, run, sleep
import os
import aiofiles
import zlib
import base64
import json

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

        await sleep(60)

        size = directory_size(path)
        if size > 10 * (1000**3):
            send_sms(f'Reaching storage limit {size / (1000**3)}g')

async def save_data(path, iso8601, method, data):

    try:

        filename = '-'.join([method, iso8601])
        async with aiofiles.open(os.path.join(path, filename), mode='w') as f:
            bytes = base64.b64encode(
                zlib.compress(
                    json.dumps(data).encode('utf-8')
                )
            ).decode('ascii')
            await f.write(bytes)

    except Exception as e:
        print(e)
        send_sms(f'{iso8601}\n\nFailed writing file')

async def symbol_loop(exchange, method, symbol, path):

    print('Starting', exchange.id, method, symbol)
    while True:
        try:
            response = await getattr(exchange, method)(symbol)
            now = exchange.milliseconds()
            iso8601 = exchange.iso8601(now)
            if method == 'watchOrderBook':
                await save_data(path, iso8601, method, response)
            elif method == 'watchTrades':
                await save_data(path, iso8601, method, response)

        except (ccxtpro.NetworkError, ccxtpro.ExchangeError, Exception) as e:

            print(e)

            send_sms(f'{iso8601}\n\nSymbol loop error')

            if type(e).__name__ == 'NetworkError':
                raise ccxtpro.NetworkError(e)

async def method_loop(exchange, method, symbols, path):
    print('Starting', exchange.id, method, symbols)
    loops = [symbol_loop(exchange, method, symbol, path) for symbol in symbols]
    await gather(*loops)


async def exchange_loop(exchange_id, methods, path):

    path = os.path.join(path, exchange_id)
    if not os.path.exists(path):
        os.mkdir(path)

    print('Starting', exchange_id, methods)
    exchange = getattr(ccxtpro, exchange_id)()
    exchange.options.update({ 'rateLimit': 10, 'watchOrderBook': { 'depth': 'books' }})
    loops = [method_loop(exchange, method, symbols, path) for method, symbols in methods.items()]
    await gather(*loops)
    await exchange.close()

async def main():

    save_path = os.path.join(os.getcwd(), '/mnt/volume_ams3_01/data')
    if not os.path.exists(save_path):
        os.mkdir(save_path)

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

    loops = [exchange_loop(exchange_id, methods, save_path) for exchange_id, methods in exchanges.items()]
    loops.append(size_monitor(save_path))
    await gather(*loops)
    
run(main())