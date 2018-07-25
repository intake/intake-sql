from intake_sql.sql_cat import SQLCatalog
from .utils import temp_db, df
import intake
import os
here = os.path.abspath(os.path.dirname(__file__))

# pytest imports this package last, so plugin is not auto-added
intake.registry['sql_cat'] = SQLCatalog


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
    cat2 = cat.tables()
    assert isinstance(cat2, SQLCatalog)
    assert 'temp' in list(cat2)
    d2 = cat.tables.temp.read()
    assert df.equals(d2)
