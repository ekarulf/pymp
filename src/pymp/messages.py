import os
from collections import namedtuple

Request = namedtuple('Request', 'id function args kwargs')
Response = namedtuple('Response', 'id exception return_value')

def generate_id(obj=None):
    if obj and hasattr(obj, 'id'):
        return obj.id
    return os.urandom(32)   # 256bit random numbers
