from ctypes import c_uint32 as unsigned_int32, c_uint8 as unsigned_byte
import time
import io
import struct
import json
import os

def zigzag_encode(n):
    return (n >> 31) ^ (n << 1)

def zigzag_decode(n):
    return (n >> 1) ^ -(n & 1)

def _byte(b):
    return bytes((b, ))

def varint_encode(number):
    buf = b''
    while True:
        towrite = number & 0x7f
        number >>= 7
        if number:
            buf += _byte(towrite | 0x80)
        else:
            buf += _byte(towrite)
            break

    return buf 

def varint_decode_stream(stream):
    shift = 0
    result = 0
    while True:
        i = _read_one(stream)  

        result |= (i & 0x7f) << shift
        shift += 7
        if not (i & 0x80):
            break

    return result

def from_twos_complement(unsigned):
    sign_mask = 1 << ((unsigned).bit_length() - 1)  
    bits_mask = sign_mask - 1        
    return (unsigned & bits_mask) - (unsigned & sign_mask)

def varint_decode(buf):
    return varint_decode_stream(io.BytesIO(buf))

def _read_one(stream):
    c = stream.read(1)
    if c == b'':
        raise EOFError('EOF')
    return ord(c)

def delta_encode(n2, n1 = 0):
    return n2 - n1

def delta_decode(n2, n1 = 0):
    return n2 + n1

def encode(current, previous = None):

    current = factorize_encode(current, 2)
    
    if previous:
        previous = factorize_encode(previous, 2)
        current = delta_encode(current, previous)

    current = zigzag_encode(current)
    current = varint_encode(current)

    return current

def decode(current, previous = None):

    current = varint_decode(current)
    current = zigzag_decode(current)

    if previous:
        current = delta_decode(current, previous)

    current = factorize_decode(current, 2)
    return current

def factorize_encode(n, precision):
    return int(n * 10**precision)

def factorize_decode(n, precision):
    return float(n) / 10**precision

# current, previous = 40000, 20000

# print(len(struct.pack('i', current)))

# current = encode(current, previous)
# print(current.bit_length() / 8)

# previous = factorize_encode(previous, 2)  # TEMP
# current = decode(current, previous)
# print(current)







