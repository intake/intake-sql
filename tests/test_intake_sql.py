import intake
import pandas as pd
import pytest

from intake_sql import (SQLSourceAutoPartition, SQLSourceManualPartition,
                        SQLSource)
from .utils import temp_db, df, df2

# pytest imports this package last, so plugin is not auto-added
intake.register_driver("sql", SQLSource, clobber=True)
intake.register_driver('sql_auto', SQLSourceAutoPartition, clobber=True)
intake.register_driver('sql_manual', SQLSourceManualPartition, clobber=True)


def test_fixture(temp_db):
    table, table_nopk, uri = temp_db
    d2 = pd.read_sql(table, uri, index_col='p')
    assert df.equals(d2)


def test_simple(temp_db):
    table, table_nopk, uri = temp_db
    d2 = SQLSource(uri, table,
                   sql_kwargs=dict(index_col='p')).read()
    assert df.equals(d2)


def test_auto(temp_db):
    table, table_nopk, uri = temp_db
    s = SQLSourceAutoPartition(uri, table, index='p',
                               sql_kwargs=dict(npartitions=2))
    assert s.discover()['npartitions'] == 2
    assert s.to_dask().npartitions == 2
    d2 = s.read()
    assert df.equals(d2)


def test_manual(temp_db):
    table, table_nopk, uri = temp_db
    s = SQLSourceManualPartition(uri, "SELECT * FROM " + table,
               where_values=['WHERE p < 20', 'WHERE p >= 20'],
               sql_kwargs=dict(index_col='p'))
    assert s.discover()['npartitions'] == 2
    assert s.to_dask().npartitions == 2
    d2 = s.read()
    assert df.equals(d2)


def test_to_ibis(temp_db):
    pytest.importorskip("ibis")
    table, table_nopk, uri = temp_db
    # Simple source with primary key
    s = SQLSource(uri, table)
    expr = s.to_ibis()
    d2 = expr.execute().set_index('p')
    assert df.equals(d2)
    # Simple source without primary key
    s = SQLSource(uri, table_nopk)
    expr = s.to_ibis()
    d_nopk = expr.execute()
    assert df2.equals(d_nopk)
    # Auto-partition (to_ibis ignores partitioning)
    s = SQLSourceAutoPartition(uri, table, index='p',
                               sql_kwargs=dict(npartitions=2))
    expr = s.to_ibis()
    d2 = expr.execute().set_index('p')
    assert df.equals(d2)
