from collections import OrderedDict
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class GeneralCache(Generic[K, V]):
    def __init__(self, max_count: int = 100):
        self._max_count: int = max_count
        self._cache: OrderedDict[K, V] = OrderedDict()

    def __contains__(self, key: K) -> bool:
        if key not in self._cache:
            return False

        self._cache.move_to_end(key)
        return True

    def __getitem__(self, key: K) -> V:
        if key not in self._cache:
            raise KeyError(key)

        self._cache.move_to_end(key)
        return self._cache[key]

    def _put(self, key: K, value: V) -> None:
        if key in self._cache:
            self._cache.move_to_end(key)

        self._cache[key] = value

        if len(self._cache) > self._max_count:
            self._cache.popitem(last=False)


class CacheDict(GeneralCache[str, V]):
    def __init__(self, max_count: int = 1000):
        super().__init__(max_count=max_count)

    def __setitem__(self, key: str, value: V) -> None:
        self._put(key, value)


class CacheSet(GeneralCache[K, None]):
    def __init__(self, max_count: int = 1000):
        super().__init__(max_count=max_count)

    def add(self, item: K) -> None:
        self._put(item, None)
