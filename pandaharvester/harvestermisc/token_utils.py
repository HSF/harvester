import hashlib

import six


def _md5sum(data):
    """
    get md5sum hexadecimal string of data
    """
    hash = hashlib.md5()
    hash.update(six.b(data))
    hash_hex = hash.hexdigest()
    return hash_hex

def endpoint_to_filename(endpoint):
    """
    get token file name according to service (CE, storage, etc.) endpoint
    currently directly take its md5sum hash as file name
    """
    filename = _md5sum(endpoint)
    return filename
