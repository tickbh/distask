from datetime import date, timedelta, tzinfo
import datetime
import re
import time
from calendar import timegm
from pytz import FixedOffset, timezone, utc

import six

RETRY_DELAY = 10

def time_now():
    return int(time.time())

def micro_now():
    return int(time.time() * 1000)

try:
    from threading import TIMEOUT_MAX
except ImportError:
    TIMEOUT_MAX = 4294967 

def datetime_to_utc_timestamp(timeval):
    """
    Converts a datetime instance to a timestamp.

    :type timeval: datetime
    :rtype: float

    """
    if timeval is not None:
        return timegm(timeval.utctimetuple()) + timeval.microsecond / 1000000


def datetime_to_timestamp(timeval):
    """
    Converts a datetime instance to a timestamp.

    :type timeval: datetime
    :rtype: float

    """
    if timeval is not None:
        return time.mktime(timeval.timetuple())

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

