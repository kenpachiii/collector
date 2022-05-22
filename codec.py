import io
import struct


def zigzag_encode(n):
    return (n >> 31) ^ (n << 1)


def zigzag_decode(n):
    return (n >> 1) ^ -(n & 1)


def _byte(b):
    return chr(b)


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


def varint_decode(buf):
    return varint_decode_stream(io.BytesIO(buf))


def _read_one(stream):
    c = stream.read(1)
    if c == b'':
        raise EOFError('EOF')
    return ord(c)


def delta_encode(n1, n2):
    return n2 - n1


def delta_decode(n1, n2):
    return n2 + n1


def encode(n1, n2):

    if isinstance(n1, float):
        n1 = encode_factorize(n1)

    delta = delta_encode(n1, n2)
    if delta <= 2**8:
        n1 = varint_encode(delta)
        n1 = zigzag_encode(n1)
    else:
        n1 = struct.pack('i', n1)

    return n1


def decode(n1, n2):
    pass


def encode_factorize(n, precision):
    return int(n**precision)


def decode_factorize(n, precision):
    return n / precision
