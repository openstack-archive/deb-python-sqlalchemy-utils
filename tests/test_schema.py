# -*- coding: utf-8 -*-
import uuid
from datetime import datetime, time, timedelta
from decimal import Decimal

import pytest
import sqlalchemy as sa
from dateutil.tz import tzoffset
from sqlalchemy.dialects.postgresql import (
    ARRAY,
    BIT,
    BYTEA,
    CIDR,
    HSTORE,
    INET,
    INTERVAL,
    MACADDR,
    OID,
    UUID,
    TSVECTOR
)
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy_utils import (
    ColorType,
    IPAddressType,
    PhoneNumber,
    PhoneNumberType,
    Schema
)
from sqlalchemy_utils.types import color  # noqa
from sqlalchemy_utils.types.ip_address import ip_address
from tests import TestCase


class TestInitializeSchema(object):
    @pytest.fixture
    def model(self):
        class MyModel(declarative_base()):
            __tablename__ = 'my_model'
            id = sa.Column(sa.Integer, primary_key=True)
            created_at = sa.Column(sa.DateTime)

        return MyModel

    def test_initialize_schema_from_table(self, model):
        types = Schema(model.__table__).types
        assert isinstance(types['id'], sa.Integer)
        assert isinstance(types['created_at'], sa.DateTime)

    def test_initialize_schema_from_class(self, model):
        types = Schema(model).types
        assert isinstance(types['id'], sa.Integer)
        assert isinstance(types['created_at'], sa.DateTime)

    def test_initialize_schema_from_mapper(self, model):
        types = Schema(model.__mapper__).types
        assert isinstance(types['id'], sa.Integer)
        assert isinstance(types['created_at'], sa.DateTime)

    def test_throws_type_error_if_unknown_types_obj_given(self):
        with pytest.raises(TypeError) as exc:
            Schema(None)
        assert (
            "Schema constructor accepts only Mapping of types, SQLAlchemy "
            "Table object, declarative class or SQLAlchemy mapper (type "
            "<type 'NoneType'> was given)."
        ) in str(exc.value)


class TestSchema(TestCase):
    def test_int(self):
        assert Schema({'key': sa.Integer()}).process(
            {'key': 1}
        ) == {'key': 1}

    def test_numeric(self):
        assert Schema({'key': sa.Numeric()}).process(
            {'key': '1.0'}
        ) == {'key': Decimal('1.0')}

    def test_float(self):
        assert Schema({'key': sa.Numeric()}).process(
            {'key': '1.0'}
        ) == {'key': 1.0}

    def test_string(self):
        assert Schema({'key': sa.String()}).process(
            {'key': u'ääää'}
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
                datetime(2011, 11, 11, 11, 11, 11, tzinfo=tzoffset(None, 7200))
            ),
        )
    )
    def test_datetime(self, value, parsed_value):
        assert Schema({'key': sa.DateTime()}).process(
            {'key': value}
        ) == {'key': parsed_value}

    def test_date(self):
        assert Schema({'key': sa.Date()}).process(
            {'key': u'2011-11-11'}
        ) == {'key': datetime(2011, 11, 11).date()}

    def test_time(self):
        assert Schema({'key': sa.Time()}).process(
            {'key': u'11:11:11'}
        ) == {'key': time(11, 11, 11)}

    @pytest.mark.parametrize(
        ('value', 'parsed_value'),
        (
            ('1 day', timedelta(days=1)),
            ('2 days', timedelta(days=2)),
            ('01:00:00', timedelta(hours=1)),
            (
                u'01:12:12.123456',
                timedelta(
                    hours=1,
                    minutes=12,
                    seconds=12,
                    microseconds=123456
                )
            ),
            ('2 years 2 months 1 day', timedelta(days=365 * 2 + 2 * 30 + 1))
        )
    )
    def test_interval(self, value, parsed_value):
        assert Schema({'key': sa.Interval()}).process(
            {'key': value}
        ) == {'key': parsed_value}

    @pytest.mark.parametrize(
        ('value'),
        (
            '00:60:00',
            '00:00:60',
        )
    )
    def test_interval_edge_cases(self, value):
        with pytest.raises(ValueError):
            Schema({'key': sa.Interval()}).process(
                {'key': value},
            )

    def test_array(self):
        assert Schema({'key': ARRAY(sa.Integer)}).process(
            {'key': [1, 2, 3]}
        ) == {'key': [1, 2, 3]}

    def test_hstore(self):
        assert Schema({'key': HSTORE()}).process(
            {'key': {'1': '2'}}
        ) == {'key': {'1': '2'}}

    def test_tsvector(self):
        assert Schema({'key': TSVECTOR()}).process(
            {'key': 'john:1 matrix:2'}
        ) == {'key': 'john:1 matrix:2'}

    def test_bit(self):
        assert Schema({'key': BIT()}).process(
            {'key': 0}
        ) == {'key': 0}

    def test_bytea(self):
        assert Schema({'key': BYTEA()}).process(
            {'key': 0}
        ) == {'key': 0}

    def test_cidr(self):
        assert Schema({'key': CIDR()}).process(
            {'key': '192.168.100.128/25'}
        ) == {'key': '192.168.100.128/25'}

    def test_enum(self):
        assert Schema({'key': sa.Enum('happy', 'sad')}).process(
            {'key': 'happy'}
        ) == {'key': 'happy'}

    def test_native_interval(self):
        assert Schema({'key': INTERVAL}).process(
            {'key': '2 days'}
        ) == {'key': timedelta(days=2)}

    def test_inet(self):
        assert Schema({'key': INET}).process(
            {'key': '192.168.0.0/25'}
        ) == {'key': '192.168.0.0/25'}

    def test_macaddr(self):
        assert Schema({'key': MACADDR}).process(
            {'key': '08002b010203'}
        ) == {'key': '08002b010203'}

    def test_oid(self):
        assert Schema({'key': OID}).process(
            {'key': '123'}
        ) == {'key': '123'}

    def test_phone_number(self):
        number = '+358111222'
        assert Schema({'key': PhoneNumberType(country_code='FI')}).process(
            {'key': number}
        ) == {'key': PhoneNumber(number, country_code='FI')}

    @pytest.mark.skipif('color.python_colour_type is None')
    def test_color(self):
        from colour import Color

        color = '#222222'
        assert Schema({'key': ColorType()}).process(
            {'key': color}
        ) == {'key': Color(color)}

    def test_ip_address(self):
        ip = u'123.123.123.123'
        assert Schema({'key': IPAddressType()}).process(
            {'key': ip}
        ) == {'key': ip_address(ip)}

    @pytest.mark.parametrize(
        'as_uuid',
        (
            False,
            True
        )
    )
    def test_uuid(self, as_uuid):
        value = 'f8018cd8-5164-4a1c-89b7-8667dcaafed8'
        assert Schema({'key': UUID(as_uuid)}).process(
            {'key': value}
        ) == {'key': uuid.UUID(value) if as_uuid else value}
