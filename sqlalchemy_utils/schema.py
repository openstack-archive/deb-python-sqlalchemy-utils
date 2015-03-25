# -*- coding: utf-8 -*-
import re
import uuid
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict  # noqa
from collections import Mapping
from datetime import timedelta
from decimal import Decimal
from inspect import isclass

import six
import sqlalchemy as sa
from dateutil.parser import parse
from sqlalchemy.dialects.postgresql import (
    ARRAY,
    BIT,
    BYTEA,
    CIDR,
    HSTORE,
    INET,
    INTERVAL,
    JSON,
    JSONB,
    MACADDR,
    OID,
    UUID,
    TSVECTOR
)

from sqlalchemy_utils import ScalarCoercible


_re_interval = re.compile(
    br"""
    (?:(-?\+?\d+)\sy\w+\s?)?    # year
    (?:(-?\+?\d+)\sm\w+\s?)?    # month
    (?:(-?\+?\d+)\sd\w+\s?)?    # day
    (?:(-?\+?)(\d+):(\d+):(\d+) # +/- hours:mins:secs
        (?:\.(\d+))?)?          # second fraction
    """,
    re.VERBOSE
)


def parse_interval(value):
    """
    Typecast an interval to a datetime.timedelta instance.
    For example, the value '2 years 1 mon 3 days 10:01:39.100' is converted
    to `datetime.timedelta(763, 36099, 100)`.

    Slightly modified version of psycopg2cffi parse_interval function.

    :param value: interval string to parse
    """
    if value is None:
        return None

    m = _re_interval.match(value)
    if not m:
        raise ValueError("Failed to parse interval: '%s'" % value)

    years, months, days, sign, hours, mins, secs, frac = m.groups()

    if mins and int(mins) > 59:
        raise ValueError(
            "Interval minutes can't be more than 59 ('{0}' given).".format(
                mins
            )
        )

    if secs and int(secs) > 59:
        raise ValueError(
            "Interval seconds can't be more than 59 ('{0}' given).".format(
                secs
            )
        )

    days = int(days) if days is not None else 0
    if months is not None:
        days += int(months) * 30
    if years is not None:
        days += int(years) * 365

    if hours is not None:
        secs = int(hours) * 3600 + int(mins) * 60 + int(secs)
        if frac is not None:
            micros = int((frac + ('0' * (6 - len(frac))))[:6])
        else:
            micros = 0

        if sign == '-':
            secs = -secs
            micros = -micros
    else:
        secs = micros = 0

    return timedelta(days, secs, micros)


identity = lambda x: x


class Schema(object):
    type_processors = OrderedDict((
        (sa.Date, lambda x: parse(x).date()),
        (sa.DateTime, parse),
        (sa.Float, float),
        (sa.Enum, identity),
        (sa.Integer, int),
        (sa.Interval, parse_interval),
        (sa.Numeric, Decimal),
        (sa.String, six.text_type),
        (sa.Time, lambda x: parse(x).time()),
        (ARRAY, list),
        (BIT, identity),
        (BYTEA, identity),
        (CIDR, identity),
        (HSTORE, dict),
        (INET, identity),
        (INTERVAL, parse_interval),
        (TSVECTOR, six.text_type),
        (JSON, identity),
        (JSONB, identity),
        (MACADDR, identity),
        (OID, identity)
    ))

    def __init__(self, types):
        if isinstance(types, sa.Table):
            self.types = dict((key, c.type) for key, c in types.c.items())
        elif isinstance(types, Mapping):
            self.types = types
        else:
            try:
                self.types = dict(
                    (key, c.type)
                    for key, c
                    in sa.inspect(types).columns.items()
                )
            except sa.exc.NoInspectionAvailable:
                raise TypeError(
                    'Schema constructor accepts only Mapping of types, '
                    'SQLAlchemy Table object, declarative class or SQLAlchemy '
                    'mapper (type {0} was given).'.format(
                        type(types)
                    )
                )

    def type_processor(self, type_):
        for sa_type, processor in self.type_processors.items():
            if (
                isinstance(type_, sa_type) or
                (
                    isclass(type_) and
                    issubclass(type_, sa_type)
                )
            ):
                return processor
            elif isinstance(type_, ScalarCoercible):
                return type_._coerce
            elif isinstance(type_, UUID):
                return uuid.UUID if type_.as_uuid else six.text_type

    def process(self, data):
        processed = {}
        for key, value in data.items():
            if key in self.types:
                processor = self.type_processor(self.types[key])
                processed[key] = processor(value)
        return processed
