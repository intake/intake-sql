from intake_sql.sql_cat import SQLCatalog
from intake_sql.tests.utils import temp_db, df
import pandas as pd


def test_cat(temp_db):
    table, uri = temp_db
    cat = SQLCatalog(uri)
    assert table in cat
    d2 = getattr(cat, table).read()
    assert df.equals(d2)
