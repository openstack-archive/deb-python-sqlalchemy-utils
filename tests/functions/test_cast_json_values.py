# -*- coding: utf-8 -*-
from datetime import datetime, time
from dateutil.parser import parse

import pytest
import six
import sqlalchemy as sa
from tests import TestCase


def int_processor(value):
    return int(value)


def type_processor(type_):
    if isinstance(type_, sa.Integer):
        return int
    elif isinstance(type_, sa.String):
        return six.text_type
    elif isinstance(type_, sa.DateTime):
        return parse
    elif isinstance(type_, sa.Date):
        return lambda x: parse(x).date()
    elif isinstance(type_, sa.Time):
        return lambda x: parse(x).time()


def cast_json_values(data, types):
    processed = {}
    for key, value in data.items():
        if key in types:
            processor = type_processor(types[key])
            processed[key] = processor(value)
    return processed


class TestCastJSONValues(TestCase):
    def test_int(self):
        assert cast_json_values(
            {'key': '1'},
            {'key': sa.Integer()}
        ) == {'key': 1}

    def test_string(self):
        assert cast_json_values(
            {'key': u'ääää'},
            {'key': sa.String()}
        ) == {'key': u'ääää'}

    @pytest.mark.parametrize(
        ('value', 'parsed_value'),
        (
            (u'2011-11-11T11:11:11', datetime(2011, 11, 11, 11, 11, 11)),
            (u'2011-11-11 11:11:11', datetime(2011, 11, 11, 11, 11, 11)),
            (
                u'2011-11-11 11:11:11.123456',
                datetime(2011, 11, 11, 11, 11, 11, 123456)
            ),
            (
                u'2011-11-11 11:11:11+02',
                datetime(2011, 11, 11, 11, 11, 11)
            ),
        )
    )
    def test_datetime(self, value, parsed_value):
        assert cast_json_values(
            {'key': value},
            {'key': sa.DateTime()}
        ) == {'key': parsed_value}

    def test_date(self):
        assert cast_json_values(
            {'key': u'2011-11-11'},
            {'key': sa.Date()}
        ) == {'key': datetime(2011, 11, 11).date()}

    def test_time(self):
        assert cast_json_values(
            {'key': u'11:11:11'},
            {'key': sa.Time()}
        ) == {'key': time(11, 11, 11)}
