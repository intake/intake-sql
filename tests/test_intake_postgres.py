import os
import pickle

import pytest
import pandas as pd

import intake_postgres as postgres
from .util import verify_plugin_interface, verify_datasource_interface


DB_URI = 'postgresql://postgres@localhost:5432/postgres'
TEST_DATA_DIR = 'tests'
TEST_DATA = [
    ('sample1_idx', 'sample1.csv', None),
    ('sample2_1_idx', 'sample2_1.csv', None),
    ('sample2_2_idx', 'sample2_2.csv', None),
    ('sample1', 'sample1.csv', 'rank'),
    ('sample_1', 'sample2_1.csv', 'rank'),
    ('sample2_2', 'sample2_2.csv', 'rank'),
]


@pytest.fixture(scope='module')
def engine():
    """Start docker container for PostgreSQL database, yield a tuple (engine,
    metadata), and cleanup connection afterward."""
    from .util import start_postgres, stop_postgres
    from sqlalchemy import create_engine
    stop_postgres(let_fail=True)
    start_postgres()

    engine = create_engine(DB_URI)
    for table_name, csv_fpath, index_col in TEST_DATA:
        df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath), index_col=index_col)
        # Dask assumes a numerical index in each table, so we use the one Pandas
        # created for us.
        if index_col is None:
            df.index.name = 'index'
        df.to_sql(table_name, engine)

    try:
        yield engine
    finally:
        stop_postgres()


def test_postgres_plugin():
    p = postgres.Plugin()
    assert isinstance(p.version, str)
    assert p.container == 'dataframe'
    verify_plugin_interface(p)


@pytest.mark.parametrize('table_name,_,index_col', TEST_DATA)
def test_open(engine, table_name, _, index_col):
    p = postgres.Plugin()
    d = p.open(DB_URI, table_name, index_col)
    assert d.container == 'dataframe'
    assert d.description is None
    verify_datasource_interface(d)


@pytest.mark.parametrize('table_name,csv_fpath,index_col', TEST_DATA)
def test_discover(engine, table_name, csv_fpath, index_col):
    expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath), index_col=index_col)
    p = postgres.Plugin()
    # Required by dd.read_sql_table(), so dask knows which column to partition
    # on. As of 04/02/17, needs to be a numerical dtype.
    if index_col is None:
        index_col = 'index'
    source = p.open(DB_URI, table_name, index_col)
    info = source.discover()
    assert info['dtype'] == list(zip(expected_df.columns, expected_df.dtypes))
    assert info['shape'] == (expected_df.shape[0],)
    assert info['npartitions'] == 1


@pytest.mark.parametrize('table_name,csv_fpath,index_col', TEST_DATA)
def test_read(engine, table_name, csv_fpath, index_col):
    expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath), index_col=index_col)
    p = postgres.Plugin()
    # Required by dd.read_sql_table(), so dask knows which column to partition
    # on. As of 04/02/17, needs to be a numerical dtype.
    if index_col is None:
        index_col = 'index'
    source = p.open(DB_URI, table_name, index_col)
    df = source.read()
    assert expected_df.equals(df)


@pytest.mark.parametrize('table_name,csv_fpath,index_col', TEST_DATA)
def test_read_chunked(engine, table_name, csv_fpath, index_col):
    expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath), index_col=index_col)

    p = postgres.Plugin()
    # Required by dd.read_sql_table(), so dask knows which column to partition
    # on. As of 04/02/17, needs to be a numerical dtype.
    if index_col is None:
        index_col = 'index'
    source = p.open(DB_URI, table_name, index_col)

    parts = list(source.read_chunked())
    df = pd.concat(parts)

    assert expected_df.equals(df)


@pytest.mark.skip('Partition support not planned')
@pytest.mark.parametrize('table_name,csv_fpath,index_col', TEST_DATA)
def test_read_partition(engine, table_name, csv_fpath, index_col):
    expected_df1 = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath), index_col=index_col)
    expected_df2 = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath), index_col=index_col)

    p = postgres.Plugin()
    # Required by dd.read_sql_table(), so dask knows which column to partition
    # on. As of 04/02/17, needs to be a numerical dtype.
    if index_col is None:
        index_col = 'index'
    source = p.open(DB_URI, table_name, index_col)

    source.discover()
    assert source.npartitions == 2

    # Read partitions is opposite order
    df2 = source.read_partition(1)
    df1 = source.read_partition(0)

    assert expected_df1.equals(df1)
    assert expected_df2.equals(df2)


@pytest.mark.parametrize('table_name,csv_fpath,index_col', TEST_DATA)
def test_to_dask(engine, table_name, csv_fpath, index_col):
    expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath), index_col=index_col)

    p = postgres.Plugin()
    # Required by dd.read_sql_table(), so dask knows which column to partition
    # on. As of 04/02/17, needs to be a numerical dtype.
    if index_col is None:
        index_col = 'index'
    source = p.open(DB_URI, table_name, index_col)

    dd = source.to_dask()
    df = dd.compute()

    assert expected_df.equals(df)


@pytest.mark.parametrize('table_name,csv_fpath,index_col', TEST_DATA)
def test_close(engine, table_name, csv_fpath, index_col):
    expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath), index_col=index_col)

    p = postgres.Plugin()
    # Required by dd.read_sql_table(), so dask knows which column to partition
    # on. As of 04/02/17, needs to be a numerical dtype.
    if index_col is None:
        index_col = 'index'
    source = p.open(DB_URI, table_name, index_col)

    source.close()
    # Can reopen after close
    df = source.read()

    assert expected_df.equals(df)


@pytest.mark.parametrize('table_name,csv_fpath,index_col', TEST_DATA)
def test_pickle(engine, table_name, csv_fpath, index_col):
    expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath), index_col=index_col)

    p = postgres.Plugin()
    # Required by dd.read_sql_table(), so dask knows which column to partition
    # on. As of 04/02/17, needs to be a numerical dtype.
    if index_col is None:
        index_col = 'index'
    source = p.open(DB_URI, table_name, index_col)

    pickled_source = pickle.dumps(source)
    source_clone = pickle.loads(pickled_source)

    expected_df = source.read()
    df = source_clone.read()

    assert expected_df.equals(df)
