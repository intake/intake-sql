import os
import pickle

import pytest
import pandas as pd

import intake_postgres as postgres
from intake.catalog import Catalog
from .util import verify_plugin_interface, verify_datasource_interface


DB_URI = 'postgresql://postgres@localhost:5432/postgres'
TEST_DATA_DIR = 'tests'
TEST_DATA = [
    ('sample1', 'sample1.csv'),
    ('sample2_1', 'sample2_1.csv'),
    ('sample2_2', 'sample2_2.csv'),
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
    for table_name, csv_fpath in TEST_DATA:
        df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
        df.to_sql(table_name, engine, index=False)

    try:
        yield engine
    finally:
        stop_postgres()


def test_postgres_plugin():
    p = postgres.Plugin()
    assert isinstance(p.version, str)
    assert p.container == 'dataframe'
    verify_plugin_interface(p)


@pytest.mark.parametrize('table_name,_', TEST_DATA)
def test_open(engine, table_name, _):
    p = postgres.Plugin()
    d = p.open(DB_URI, 'select * from '+table_name)
    assert d.container == 'dataframe'
    assert d.description is None
    verify_datasource_interface(d)


@pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
def test_discover(engine, table_name, csv_fpath):
    expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
    p = postgres.Plugin()
    source = p.open(DB_URI, 'select * from '+table_name)
    info = source.discover()
    assert info['dtype'] == list(zip(expected_df.columns, expected_df.dtypes))
    assert info['shape'] == (None, 3)
    assert info['npartitions'] == 1


@pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
def test_read(engine, table_name, csv_fpath):
    expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
    p = postgres.Plugin()
    source = p.open(DB_URI, 'select * from '+table_name)
    df = source.read()
    assert expected_df.equals(df)


@pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
def test_discover_after_read(engine, table_name, csv_fpath):
    """Assert that after reading the dataframe, discover() shows more accurate
    information.
    """
    expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
    p = postgres.Plugin()
    source = p.open(DB_URI, 'select * from '+table_name)
    info = source.discover()
    assert info['dtype'] == list(zip(expected_df.columns, expected_df.dtypes))
    assert info['shape'] == (None, 3)
    assert info['npartitions'] == 1

    df = source.read()
    assert expected_df.equals(df)

    info = source.discover()
    assert info['dtype'] == list(zip(expected_df.columns, expected_df.dtypes))
    assert info['shape'] == (4, 3)
    assert info['npartitions'] == 1

    assert expected_df.equals(df)


@pytest.mark.skip('Not implemented yet')
@pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
def test_read_chunked(engine, table_name, csv_fpath):
    expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))

    p = postgres.Plugin()
    source = p.open(DB_URI, 'select * from '+table_name)

    parts = list(source.read_chunked())
    df = pd.concat(parts)

    assert expected_df.equals(df)


@pytest.mark.skip('Partition support not planned')
@pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
def test_read_partition(engine, table_name, csv_fpath):
    expected_df1 = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
    expected_df2 = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))

    p = postgres.Plugin()
    source = p.open(DB_URI, 'select * from '+table_name)

    source.discover()
    assert source.npartitions == 2

    # Read partitions is opposite order
    df2 = source.read_partition(1)
    df1 = source.read_partition(0)

    assert expected_df1.equals(df1)
    assert expected_df2.equals(df2)


@pytest.mark.skip('Not implemented yet')
@pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
def test_to_dask(engine, table_name, csv_fpath):
    expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))

    p = postgres.Plugin()
    source = p.open(DB_URI, 'select * from '+table_name)

    dd = source.to_dask()
    df = dd.compute()

    assert expected_df.equals(df)


@pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
def test_close(engine, table_name, csv_fpath):
    expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))

    p = postgres.Plugin()
    source = p.open(DB_URI, 'select * from '+table_name)

    source.close()
    # Can reopen after close
    df = source.read()

    assert expected_df.equals(df)


@pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
def test_pickle(engine, table_name, csv_fpath):
    expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))

    p = postgres.Plugin()
    source = p.open(DB_URI, 'select * from '+table_name)

    pickled_source = pickle.dumps(source)
    source_clone = pickle.loads(pickled_source)

    expected_df = source.read()
    df = source_clone.read()

    assert expected_df.equals(df)


@pytest.mark.parametrize('table_name,_1', TEST_DATA)
def test_catalog(engine, table_name, _1):
    catalog_fpath = os.path.join(TEST_DATA_DIR, 'catalog1.yml')

    catalog = Catalog(catalog_fpath)
    ds_name = table_name.rsplit('_idx', 1)[0]
    src = catalog[ds_name]
    pgsrc = src.get()

    assert src.describe()['container'] == 'dataframe'
    assert src.describe_open()['plugin'] == 'postgres'
    assert src.describe_open()['args']['sql_expr'][:6] in ('select', 'SELECT')

    metadata = pgsrc.discover()
    assert metadata['npartitions'] == 1

    expected_df = pd.read_sql_query(pgsrc._sql_expr, engine)
    df = pgsrc.read()
    assert expected_df.equals(df)

    pgsrc.close()


def test_catalog_join(engine):
    catalog_fpath = os.path.join(TEST_DATA_DIR, 'catalog1.yml')

    catalog = Catalog(catalog_fpath)
    ds_name = 'sample2'
    src = catalog[ds_name]
    pgsrc = src.get()

    assert src.describe()['container'] == 'dataframe'
    assert src.describe_open()['plugin'] == 'postgres'
    assert src.describe_open()['args']['sql_expr'][:6] in ('select', 'SELECT')

    metadata = pgsrc.discover()
    assert metadata['npartitions'] == 1

    expected_df = pd.read_sql_query(pgsrc._sql_expr, engine)
    df = pgsrc.read()
    assert expected_df.equals(df)

    pgsrc.close()
