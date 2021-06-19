from datetime import date, timedelta, tzinfo
import datetime
import re
import time
from calendar import timegm
from pytz import FixedOffset, timezone, utc

import string
from inspect import isclass, ismethod
from functools import partial
import six

RETRY_DELAY = 10
        
def time_now():
    return int(time.time())

def micro_now():
    return int(time.time() * 1000)

# 2050年1月1日
def micro_max():
    return int(2524579200_000)

try:
    from threading import TIMEOUT_MAX
except ImportError:
    TIMEOUT_MAX = 4294967 

def safeint(text):
    if text is not None:
        return int(text)


def datetime_ceil(dateval):
    """
    Rounds the given datetime object upwards.

    :type dateval: datetime

    """
    if dateval.microsecond > 0:
        return dateval + timedelta(seconds=1, microseconds=-dateval.microsecond)
    return dateval



_DATE_REGEX = re.compile(
    r'(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})'
    r'(?:[ T](?P<hour>\d{1,2}):(?P<minute>\d{1,2}):(?P<second>\d{1,2})'
    r'(?:\.(?P<microsecond>\d{1,6}))?'
    r'(?P<timezone>Z|[+-]\d\d:\d\d)?)?$')

def convert_to_datetime(input, tz, arg_name):
    """
    Converts the given object to a timezone aware datetime object.

    If a timezone aware datetime object is passed, it is returned unmodified.
    If a native datetime object is passed, it is given the specified timezone.
    If the input is a string, it is parsed as a datetime with the given timezone.

    Date strings are accepted in three different forms: date only (Y-m-d), date with time
    (Y-m-d H:M:S) or with date+time with microseconds (Y-m-d H:M:S.micro). Additionally you can
    override the time zone by giving a specific offset in the format specified by ISO 8601:
    Z (UTC), +HH:MM or -HH:MM.

    :param str|datetime input: the datetime or string to convert to a timezone aware datetime
    :param datetime.tzinfo tz: timezone to interpret ``input`` in
    :param str arg_name: the name of the argument (used in an error message)
    :rtype: datetime

    """
    if input is None:
        return
    elif isinstance(input, datetime):
        datetime_ = input
    elif isinstance(input, date):
        datetime_ = datetime.combine(input, time())
    elif isinstance(input, six.string_types):
        m = _DATE_REGEX.match(input)
        if not m:
            raise ValueError('Invalid date string')

        values = m.groupdict()
        tzname = values.pop('timezone')
        if tzname == 'Z':
            tz = utc
        elif tzname:
            hours, minutes = (int(x) for x in tzname[1:].split(':'))
            sign = 1 if tzname[0] == '+' else -1
            tz = FixedOffset(sign * (hours * 60 + minutes))

        values = {k: int(v or 0) for k, v in values.items()}
        datetime_ = datetime(**values)
    else:
        raise TypeError('Unsupported type for %s: %s' % (arg_name, input.__class__.__name__))

    if datetime_.tzinfo is not None:
        return datetime_
    if tz is None:
        raise ValueError(
            'The "tz" argument must be specified if %s has no timezone information' % arg_name)
    if isinstance(tz, six.string_types):
        tz = timezone(tz)

    try:
        return tz.localize(datetime_, is_dst=None)
    except AttributeError:
        raise TypeError(
            'Only pytz timezones are supported (need the localize() and normalize() methods)')


def datetime_repr(dateval):
    return dateval.strftime('%Y-%m-%d %H:%M:%S %Z') if dateval else 'None'


def astimezone(obj):
    """
    Interprets an object as a timezone.

    :rtype: tzinfo

    """
    if isinstance(obj, six.string_types):
        return timezone(obj)
    if isinstance(obj, tzinfo):
        if not hasattr(obj, 'localize') or not hasattr(obj, 'normalize'):
            raise TypeError('Only timezones from the pytz library are supported')
        if obj.zone == 'local':
            raise ValueError(
                'Unable to determine the name of the local timezone -- you must explicitly '
                'specify the name of the local timezone. Please refrain from using timezones like '
                'EST to prevent problems with daylight saving time. Instead, use a locale based '
                'timezone name (such as Europe/Helsinki).')
        return obj
    if obj is not None:
        raise TypeError('Expected tzinfo, got %s instead' % obj.__class__.__name__)


def get_callable_name(func):
    """
    Returns the best available display name for the given function/callable.

    :rtype: str

    """
    # the easy case (on Python 3.3+)
    if hasattr(func, '__qualname__'):
        return func.__qualname__

    # class methods, bound and unbound methods
    f_self = getattr(func, '__self__', None) or getattr(func, 'im_self', None)
    if f_self and hasattr(func, '__name__'):
        f_class = f_self if isclass(f_self) else f_self.__class__
    else:
        f_class = getattr(func, 'im_class', None)

    if f_class and hasattr(func, '__name__'):
        return '%s.%s' % (f_class.__name__, func.__name__)

    # class or class instance
    if hasattr(func, '__call__'):
        # class
        if hasattr(func, '__name__'):
            return func.__name__

        # instance of a class with a __call__ method
        return func.__class__.__name__

    raise TypeError('Unable to determine a name for %r -- maybe it is not a callable?' % func)


def obj_to_ref(obj):
    """
    Returns the path to the given callable.

    :rtype: str
    :raises TypeError: if the given object is not callable
    :raises ValueError: if the given object is a :class:`~functools.partial`, lambda or a nested
        function

    """
    if isinstance(obj, partial):
        raise ValueError('Cannot create a reference to a partial()')

    name = get_callable_name(obj)
    if '<lambda>' in name:
        raise ValueError('Cannot create a reference to a lambda')
    if '<locals>' in name:
        raise ValueError('Cannot create a reference to a nested function')

    if ismethod(obj):
        if hasattr(obj, 'im_self') and obj.im_self:
            # bound method
            module = obj.im_self.__module__
        elif hasattr(obj, 'im_class') and obj.im_class:
            # unbound method
            module = obj.im_class.__module__
        else:
            module = obj.__module__
    else:
        module = obj.__module__
    return '%s:%s' % (module, name)


def ref_to_obj(ref):
    """
    Returns the object pointed to by ``ref``.

    :type ref: str

    """
    if not isinstance(ref, six.string_types):
        raise TypeError('References must be strings')
    if ':' not in ref:
        raise ValueError('Invalid reference')

    modulename, rest = ref.split(':', 1)
    try:
        obj = __import__(modulename, fromlist=[rest])
    except ImportError:
        raise LookupError('Error resolving reference %s: could not import module' % ref)

    try:
        for name in rest.split('.'):
            obj = getattr(obj, name)
        return obj
    except Exception:
        raise LookupError('Error resolving reference %s: error looking up object' % ref)

def safe_int(num):
    try:
        return int(num)
    except ValueError:
        result = "0"
        for c in num:
            if c not in string.digits:
                break
            result += c
        return int(result)

def bytes_to_str(b):
    if type(b) == str:
        return b
    return bytes.decode(b, "utf-8")

def bytes_to_int(b):
    if type(b) == int:
        return b
    s = bytes_to_str(b)
    return safe_int(s)

def str_to_bytes(s):
    if type(s) == bytes:
        return s
    return s.encode(encoding="utf-8")

def asint(text):
    if text is not None:
        return int(text)


# import signal
# from contextlib import contextmanager

# class TimeoutException(Exception): pass

# @contextmanager
# def time_limit(seconds):
#     def signal_handler(signum, frame):
#         raise TimeoutException("Timed out!")
#     signal.signal(signal.SIGALRM, signal_handler)
#     signal.alarm(seconds)
#     try:
#         yield
#     finally:
#         signal.alarm(0)


# import threading

# class TimeoutError(Exception):
#     pass

# class InterruptableThread(threading.Thread):
#     def __init__(self, func, *args, **kwargs):
#         threading.Thread.__init__(self)
#         self._func = func
#         self._args = args
#         self._kwargs = kwargs
#         self._result = None

#     def run(self):
#         self._result = self._func(*self._args, **self._kwargs)

#     @property
#     def result(self):
#         return self._result

# def timeout_limit(sec, f, *args, **kwargs):
#     it = InterruptableThread(f, *args, **kwargs)
#     it.start()
#     it.join(sec)
#     if not it.is_alive():
#         return it.result
#     raise TimeoutError('execution expired')
