import json
import logging
import random
import time
import uuid
from functools import wraps
from typing import Any, Callable, Iterable, Generator

from redis import Redis
from redis.exceptions import ConnectionError, TimeoutError

try:
    import _pickle as pickle
except ImportError:
    import pickle


class RedisCache(object):
    default_encoding = 'utf-8'

    def __init__(self, cfg):
        self._redis = Redis(**cfg)
        # self.logger = logging.getLogger(self.__class__.__name__)

    redis = property(lambda self: self._redis)

    @staticmethod
    def pickle_encoder(obj: Any) -> bytes:
        return pickle.dumps(obj)

    @staticmethod
    def pickle_decoder(ret: bytes) -> Any:
        return pickle.loads(ret)

    @classmethod
    def json_encoder(cls, obj: Any) -> bytes:
        return json.dumps(obj)

    @classmethod
    def json_decoder(cls, ret: bytes) -> Any:
        return json.loads(ret, encoding=cls.default_encoding)

    def cache(self, key: Any, encoder: Callable[[Any], bytes], decoder: Callable[[bytes], Any], ex: Any = None):
        def _wraps(func):
            @wraps(func)
            def _do(*args, **kwargs):
                ck = key(*args, **kwargs) if callable(key) else key
                data = self._redis.get(ck)
                if data:
                    # self.logger.debug("cache hit %s", key)
                    return decoder(data)
                ret = func(*args, **kwargs)
                data = encoder(ret)
                # self.logger.debug("cache set %s", key)
                self._redis.set(ck, data, ex=ex)
                return ret
            return _do

        return _wraps

    def force_cache(self, key: Any, encoder: Callable[[Any], bytes] = pickle_encoder, ex: Any = None):
        def _wraps(func):
            @wraps(func)
            def _do(*args, **kwargs):
                ck = key(*args, **kwargs) if callable(key) else key
                ret = func(*args, **kwargs)
                data = encoder(ret)
                # self.logger.debug("force update cache %s", ck)
                self._redis.set(ck, data, ex=ex)
                return ret
            return _do

        return _wraps

    def json_cache(self, key: Any, ex: Any = None):
        return self.cache(key, self.json_encoder, self.json_decoder, ex)

    def pickle_cache(self, key: Any, ex: Any = None):
        return self.cache(key, self.pickle_encoder, self.pickle_decoder, ex)

    def remove(self, key: Any, by_return: bool = False):
        def _wraps(func):
            @wraps(func)
            def _do(*args, **kwargs):
                ret = func(*args, **kwargs)
                if by_return:
                    keys = map(key, ret) if isinstance(ret, Generator) else (key(ret),)
                else:
                    _k = key(*args, **kwargs) if callable(key) else key
                    keys = (_k,)
                self._remove(keys)
            return _do
        return _wraps

    @staticmethod
    def _split_iterable(keys: Iterable[str], size: int = 10):
        buf = []
        for key in keys:
            buf.append(key)
            if len(key) >= size:
                yield buf
                buf.clear()
        if buf:
            yield buf

    def _remove(self, keys: Iterable[str], size: int = 10):
        for names in self._split_iterable(keys, size):
            self._redis.delete(*names)

