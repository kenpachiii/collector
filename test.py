import random
import codec
import time
import json
import sys
import os
import tempfile
import lzma
import numpy as np

class OrderBook:
    def __init__(self, *args):

        if isinstance(*args, bytes):
            self.parse(*args)
            return

        self.bids: list = args[0].get('bids').copy()
        self.asks: list = args[0].get('asks').copy()
        self.timestamp: int = args[0].get('timestamp') or int(time.time() * 1000)

    def format(self, prev):

        buf = b''

        buf += codec.varint_encode(codec.delta_encode(self.timestamp, getattr(prev, 'timestamp', 0))) + codec._byte(ord(';'))

        for i in range(0, len(self.bids)):
            packed = codec.encode(self.bids[i][0]) + codec._byte(ord(' ')) + codec.encode(self.bids[i][1])
            buf += packed + codec._byte(ord(','))

        buf += codec._byte(ord(';'))

        for i in range(0, len(self.asks)):
            packed = codec.encode(self.asks[i][0]) + codec._byte(ord(' ')) + codec.encode(self.asks[i][1])
            buf += packed + codec._byte(ord(','))

        return buf

    def parse(self, book: bytes):

        parts = book.split(b';')
        for i in range(0, len(parts)):

            if i == 0:
                setattr(self, 'timestamp', codec.delta_decode(codec.varint_decode(parts[i])))

            if i == 1:
                bids = [x for x in parts[i].split(b',')]
                bids = [x.split(b' ') for x in bids]

                setattr(self, 'bids', bids)

            if i == 2:
                asks = [x for x in parts[i].split(b',')]
                asks = [x.split(b' ') for x in asks]
                setattr(self, 'asks', asks)

        return self

class Trade:
    def __init__(self, *args):

        if isinstance(*args, bytes):
            self.parse(*args)
            return

        self.id: int = int(args[0].get('id'))
        self.price: float = args[0].get('price')
        self.amount: float = args[0].get('amount')
        self.side: str = args[0].get('side')
        self.timestamp: int = args[0].get('timestamp') or int(time.time() * 1000)

    def format(self, prev = None):

        buf = b''

        buf += codec._byte(0x0) if self.side == 'buy' else codec._byte(0x01) + codec._byte(ord(';'))

        buf += codec.varint_encode(codec.delta_encode(self.id, getattr(prev, 'id', 0))) + codec._byte(ord(';'))
        buf += codec.varint_encode(codec.delta_encode(self.timestamp, getattr(prev, 'timestamp', 0))) + codec._byte(ord(';'))
        buf += codec.encode(self.price, getattr(prev, 'price', 0)) + codec._byte(ord(';'))
        buf += codec.encode(self.amount, getattr(prev, 'amount', 0))

        return buf

    def parse(self, trade: bytes):

        parts = trade.split(b';')
        for i in range(0, len(parts)):

            if i == 0:
                setattr(self, 'side', 'buy') if parts[i] == 0x0 else setattr(self, 'side', 'sell')

            if i == 1:
                setattr(self, 'id', codec.delta_decode(codec.varint_decode(parts[i])))

            if i == 2:
                setattr(self, 'timestamp', codec.delta_decode(codec.varint_decode(parts[i])))

            if i == 3:
                setattr(self, 'price', codec.decode(parts[i]))

            if i == 4:
                setattr(self, 'amount', codec.decode(parts[i]))

# order_book 4440
# trade 336
# for key, value in { 'order_book': json.loads(open('order_book.json').read()), 'trade': json.loads(open('trade.json').read()) }.items():

#     prev = None
#     for v in value:

#         print(f'starting size of {key} {sys.getsizeof(v)} bytes')

#         if key == 'order_book':
#             c = OrderBook

#         if key == 'trade':
#             c = Trade

#         start = time.time() * 1000

#         nbytes = c(v).format(prev)

#         prev = c(v)

#         print('completed {} format in {} ms with {} bytes'.format(key, (time.time() * 1000) - start, len(nbytes)))


#         start = time.time() * 1000

#         c(nbytes)

#         print('completed {} parse in {} ms'.format(key, (time.time() * 1000) - start))

# np.argwhere(int(trade.timestamp) >= order_book_keys)

import pandas as pd
import lzma

file = './data/okx/trades/BTC-USD-BTC/2022-05-29.xz'

start = time.time() * 1000

df = pd.read_json('./data/okx/trades/BTC-USD-BTC/2022-05-29.xz', lines = True)
print(df)



print('completed in {} ms'.format((time.time() * 1000) - start))