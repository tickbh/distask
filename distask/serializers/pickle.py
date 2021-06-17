from dataclasses import dataclass
try:
    from dill import dumps, loads
except:
    print("import error bill")
    from pickle import dumps, loads

from distask.serializers.base import Serializer


@dataclass(frozen=True)
class PickleSerializer(Serializer):
    protocol: int = 4

    def serialize(self, obj) -> bytes:
        return dumps(obj, self.protocol)

    def deserialize(self, serialized: bytes):
        return loads(serialized)
