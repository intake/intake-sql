import intake
from intake_sql import (SQLSourceAutoPartition, SQLSourceManualPartition,
                        SQLSource)
from .utils import temp_db, df
import pandas as pd

# pytest imports this package last, so plugin is not auto-added
intake.registry['sql'] = SQLSource
intake.registry['sql_auto'] = SQLSourceAutoPartition
intake.registry['sql_manual'] = SQLSourceManualPartition


def test_fixture(temp_db):
    table, uri = temp_db
    d2 = pd.read_sql(table, uri, index_col='p')
    assert df.equals(d2)


def test_simple(temp_db):
    table, uri = temp_db
    d2 = SQLSource(uri, table,
                   sql_kwargs=dict(index_col='p')).read()
    assert df.equals(d2)


def test_auto(temp_db):
    table, uri = temp_db
    s = SQLSourceAutoPartition(uri, table, index='p',
                               sql_kwargs=dict(npartitions=2))
    assert s.discover()['npartitions'] == 2
    assert s.to_dask().npartitions == 2
    d2 = s.read()
    assert df.equals(d2)


def test_manual(temp_db):
    table, uri = temp_db
    s = SQLSourceManualPartition(uri, "SELECT * FROM " + table,
               where_values=['WHERE p < 20', 'WHERE p >= 20'],
               sql_kwargs=dict(index_col='p'))
    assert s.discover()['npartitions'] == 2
    assert s.to_dask().npartitions == 2
    d2 = s.read()
    assert df.equals(d2)


def test_to_ibis(temp_db):
    table, uri = temp_db
    # Simple source
    s = SQLSource(uri, table)
    expr = s.to_ibis()
    d2 = expr.execute().set_index('p')
    assert df.equals(d2)
    # Auto-partition (to_ibis ignores partitioning)
    s = SQLSourceAutoPartition(uri, table, index='p',
                               sql_kwargs=dict(npartitions=2))
    expr = s.to_ibis()
    d2 = expr.execute().set_index('p')
    assert df.equals(d2)
    # Manual-partition (to_ibis ignores partitioning)
    s = SQLSourceManualPartition(uri, table,
               where_values=['WHERE p < 20', 'WHERE p >= 20'],
               sql_kwargs=dict(index_col='p'))
    expr = s.to_ibis()
    d2 = expr.execute().set_index('p')
    assert df.equals(d2)
