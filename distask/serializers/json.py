from dataclasses import dataclass, field
from json import dumps, loads
from typing import Any, Callable, Dict, Tuple

from distask.serializers.base import Serializer
from distask.task import DeserializationError

def callable_from_ref(ref: str) -> Callable:
    """
    Return the callable pointed to by ``ref``.

    :raises DeserializationError: if the reference could not be resolved or the looked up object is
        not callable

    """
    if ':' not in ref:
        raise ValueError(f'Invalid reference: {ref}')

    modulename, rest = ref.split(':', 1)
    try:
        obj = __import__(modulename, fromlist=[rest])
    except ImportError:
        raise LookupError(f'Error resolving reference {ref!r}: could not import module')

    try:
        for name in rest.split('.'):
            obj = getattr(obj, name)
    except Exception:
        raise DeserializationError(f'Error resolving reference {ref!r}: error looking up object')

    if not callable(obj):
        raise DeserializationError(f'{ref!r} points to an object of type '
                                   f'{obj.__class__.__qualname__} which is not callable')

    return obj

def marshal_object(obj) -> Tuple[str, Any]:
    return f'{obj.__class__.__module__}:{obj.__class__.__qualname__}', obj.__getstate__()


def unmarshal_object(ref: str, state):
    cls = callable_from_ref(ref)
    instance = cls.__new__(cls)
    instance.__setstate__(state)
    return instance

@dataclass
class JSONSerializer(Serializer):
    magic_key: str = '_distask_json'
    dump_options: Dict[str, Any] = field(default_factory=dict)
    load_options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.dump_options['default'] = self._default_hook
        self.load_options['object_hook'] = self._object_hook

    @classmethod
    def _default_hook(cls, obj):
        if hasattr(obj, '__getstate__'):
            cls_ref, state = marshal_object(obj)
            return {cls.magic_key: [cls_ref, state]}

        raise TypeError(f'Object of type {obj.__class__.__name__!r} is not JSON serializable')

    @classmethod
    def _object_hook(cls, obj_state: Dict[str, Any]):
        if cls.magic_key in obj_state:
            ref, *rest = obj_state[cls.magic_key]
            return unmarshal_object(ref, *rest)

        return obj_state

    def serialize(self, obj) -> bytes:
        return dumps(obj, ensure_ascii=False, **self.dump_options).encode('utf-8')

    def deserialize(self, serialized: bytes):
        return loads(serialized, **self.load_options)

    def serialize_to_unicode(self, obj) -> str:
        return dumps(obj, ensure_ascii=False, **self.dump_options)

    def deserialize_from_unicode(self, serialized: str):
        return loads(serialized, **self.load_options)
