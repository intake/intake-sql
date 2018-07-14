from intake_sql.sql_cat import SQLCatalog
from intake_sql.tests.utils import temp_db, df
import intake
import os
here = os.path.abspath(os.path.dirname(__file__))


def test_cat(temp_db):
    table, uri = temp_db
    cat = SQLCatalog(uri)
    assert table in cat
    d2 = getattr(cat, table).read()
    assert df.equals(d2)


def test_yaml_cat(temp_db):
    table, uri = temp_db
    os.environ['TEST_SQLITE_URI'] = uri  # used in catalog default
    cat = intake.Catalog(os.path.join(here, 'cat.yaml'))
    assert 'tables' in cat
    cat2 = cat.tables
    assert isinstance(cat2, SQLCatalog)
    d2 = getattr(cat2, table).read()
    assert df.equals(d2)
