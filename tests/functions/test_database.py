import os

import sqlalchemy as sa
from flexmock import flexmock
from pytest import mark

from sqlalchemy_utils import create_database, database_exists, drop_database
from tests import TestCase

pymysql = None
try:
    import pymysql  # noqa
except ImportError:
    pass


class DatabaseTest(TestCase):
    def test_create_and_drop(self):
        assert not database_exists(self.url)
        create_database(self.url)
        assert database_exists(self.url)
        drop_database(self.url)
        assert not database_exists(self.url)


class TestDatabaseSQLite(DatabaseTest):
    url = 'sqlite:///sqlalchemy_utils.db'

    def setup(self):
        if os.path.exists('sqlalchemy_utils.db'):
            os.remove('sqlalchemy_utils.db')

    def test_exists_memory(self):
        assert database_exists('sqlite:///:memory:')


@mark.skipif('pymysql is None')
class TestDatabaseMySQL(DatabaseTest):
    url = 'mysql+pymysql://travis@localhost/db_test_sqlalchemy_util'


@mark.skipif('pymysql is None')
class TestDatabaseMySQLWithQuotedName(DatabaseTest):
    url = 'mysql+pymysql://travis@localhost/db_test_sqlalchemy-util'


class TestDatabasePostgresWithQuotedName(DatabaseTest):
    url = 'postgres://postgres@localhost/db_test_sqlalchemy-util'

    def test_template(self):
        (
            flexmock(sa.engine.Engine)
            .should_receive('execute')
            .with_args(
                '''CREATE DATABASE "db_test_sqlalchemy-util"'''
                " ENCODING 'utf8' "
                'TEMPLATE "my-template"'
            )
        )
        create_database(
            'postgres://postgres@localhost/db_test_sqlalchemy-util',
            template='my-template'
        )


class TestDatabasePostgres(DatabaseTest):
    url = 'postgres://postgres@localhost/db_test_sqlalchemy_util'

    def test_template(self):
        (
            flexmock(sa.engine.Engine)
            .should_receive('execute')
            .with_args(
                "CREATE DATABASE db_test_sqlalchemy_util ENCODING 'utf8' "
                "TEMPLATE my_template"
            )
        )
        create_database(
            'postgres://postgres@localhost/db_test_sqlalchemy_util',
            template='my_template'
        )
