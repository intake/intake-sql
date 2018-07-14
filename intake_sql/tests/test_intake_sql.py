from intake_sql import SQLManualPartition, SQLAutoPartitionPlugin, SQLPlugin
from intake_sql.tests.utils import temp_db, df
import pandas as pd


def test_fixture(temp_db):
    table, uri = temp_db
    d2 = pd.read_sql(table, uri, index_col='p')
    assert df.equals(d2)


def test_simple(temp_db):
    table, uri = temp_db
    p = SQLPlugin()
    d2 = p.open(uri, table, index_col='p').read()
    assert df.equals(d2)


def test_auto(temp_db):
    table, uri = temp_db
    p = SQLAutoPartitionPlugin()
    s = p.open(uri, table, index='p', npartitions=2)
    assert s.discover()['npartitions'] == 2
    assert s.to_dask().npartitions == 2
    d2 = s.read()
    assert df.equals(d2)


def test_manual(temp_db):
    table, uri = temp_db
    p = SQLManualPartition()
    s = p.open(uri, "SELECT * FROM " + table,
               where_values=['WHERE p < 20', 'WHERE p >= 20'],
               index_col='p')
    assert s.discover()['npartitions'] == 2
    assert s.to_dask().npartitions == 2
    d2 = s.read()
    assert df.equals(d2)
