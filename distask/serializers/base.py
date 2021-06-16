
from abc import ABCMeta, abstractmethod
from base64 import b64decode, b64encode
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, FrozenSet, Iterable, Iterator, List, Optional, Set, Type
from datetime import datetime, timedelta

class Serializer(metaclass=ABCMeta):
    __slots__ = ()

    @abstractmethod
    def serialize(self, obj) -> bytes:
        pass

    def serialize_to_unicode(self, obj) -> str:
        return b64encode(self.serialize(obj)).decode('ascii')

    @abstractmethod
    def deserialize(self, serialized: bytes):
        pass

    def deserialize_from_unicode(self, serialized: str):
        return self.deserialize(b64decode(serialized))
