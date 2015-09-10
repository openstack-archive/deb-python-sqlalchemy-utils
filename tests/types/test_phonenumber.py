import six
import sqlalchemy as sa
from pytest import mark

from sqlalchemy_utils import PhoneNumber, PhoneNumberType, types  # noqa
from tests import TestCase


@mark.skipif('types.phone_number.phonenumbers is None')
class TestPhoneNumber(object):
    def setup_method(self, method):
        self.valid_phone_numbers = [
            '040 1234567',
            '+358 401234567',
            '09 2501234',
            '+358 92501234',
            '0800 939393',
            '09 4243 0456',
            '0600 900 500'
        ]
        self.invalid_phone_numbers = [
            'abc',
            '+040 1234567',
            '0111234567',
            '358'
        ]

    def test_valid_phone_numbers(self):
        for raw_number in self.valid_phone_numbers:
            number = PhoneNumber(raw_number, 'FI')
            assert number.is_valid_number()

    def test_invalid_phone_numbers(self):
        for raw_number in self.invalid_phone_numbers:
            try:
                number = PhoneNumber(raw_number, 'FI')
                assert not number.is_valid_number()
            except:
                pass

    def test_phone_number_attributes(self):
        number = PhoneNumber('+358401234567')
        assert number.e164 == u'+358401234567'
        assert number.international == u'+358 40 1234567'
        assert number.national == u'040 1234567'

    def test_phone_number_str_repr(self):
        number = PhoneNumber('+358401234567')
        if six.PY2:
            assert unicode(number) == number.national  # noqa
            assert str(number) == number.national.encode('utf-8')
        else:
            assert str(number) == number.national


@mark.skipif('types.phone_number.phonenumbers is None')
class TestPhoneNumberType(TestCase):

    def create_models(self):
        class User(self.Base):
            __tablename__ = 'user'
            id = sa.Column(sa.Integer, autoincrement=True, primary_key=True)
            name = sa.Column(sa.Unicode(255))
            phone_number = sa.Column(PhoneNumberType())

        self.User = User

    def setup_method(self, method):
        super(TestPhoneNumberType, self).setup_method(method)
        self.phone_number = PhoneNumber(
            '040 1234567',
            'FI'
        )
        self.user = self.User()
        self.user.name = u'Someone'
        self.user.phone_number = self.phone_number
        self.session.add(self.user)
        self.session.commit()

    def test_query_returns_phone_number_object(self):
        queried_user = self.session.query(self.User).first()
        assert queried_user.phone_number == self.phone_number

    def test_phone_number_is_stored_as_string(self):
        result = self.session.execute(
            'SELECT phone_number FROM "user" WHERE id=:param',
            {'param': self.user.id}
        )
        assert result.first()[0] == u'+358401234567'

    def test_phone_number_with_extension(self):
        user = self.User(phone_number='555-555-5555 Ext. 555')

        self.session.add(user)
        self.session.commit()
        self.session.refresh(user)
        assert user.phone_number.extension == '555'

    def test_empty_phone_number_is_equiv_to_none(self):
        user = self.User(phone_number='')

        self.session.add(user)
        self.session.commit()
        self.session.refresh(user)
        assert user.phone_number is None

    def test_phone_number_is_none(self):
        phone_number = None
        user = self.User()
        user.name = u'Someone'
        user.phone_number = phone_number
        self.session.add(user)
        self.session.commit()
        queried_user = self.session.query(self.User)[1]
        assert queried_user.phone_number is None
        result = self.session.execute(
            'SELECT phone_number FROM "user" WHERE id=:param',
            {'param': user.id}
        )
        assert result.first()[0] is None

    def test_scalar_attributes_get_coerced_to_objects(self):
        user = self.User(phone_number='050111222')

        assert isinstance(user.phone_number, PhoneNumber)
