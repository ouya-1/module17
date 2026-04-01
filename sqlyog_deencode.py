import base64

def sqlyog_decode(base64str):
    tmp = base64.b64decode(base64str)
    return bytearray([(b << 1 & 255) | (b >> 7) for b in tmp]).decode("utf8")


def sqlyog_encode(text):
    tmp = text.encode("utf8")
    return base64.b64encode(bytearray([(b >> 1) | ((b & 1) << 7) for b in tmp])).decode("utf8")
