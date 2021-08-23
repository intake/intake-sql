import os
import random
import requests
import subprocess
import time

import intake
import pytest

from intake_sql.sql_cat import SQLCatalog
from .utils import temp_db, df, df2

here = os.path.abspath(os.path.dirname(__file__))

# pytest imports this package last, so plugin is not auto-added
intake.registry['sql_cat'] = SQLCatalog


def test_cat(temp_db):
    table, table_nopk, uri = temp_db
    cat = SQLCatalog(uri)
    assert table in cat
    assert table_nopk in cat
    d2 = getattr(cat, table).read()
    assert df.equals(d2)
    d_noindex = getattr(cat, table_nopk).read()
    assert df2.equals(d_noindex)


def test_yaml_cat(temp_db):
    table, table_nopk, uri = temp_db
    os.environ['TEST_SQLITE_URI'] = uri  # used in catalog default
    cat = intake.open_catalog(os.path.join(here, 'cat.yaml'))
    import pdb
    pdb.set_trace()
    assert 'tables' in cat
    cat2 = cat.tables()
    assert isinstance(cat2, SQLCatalog)
    assert table in list(cat2)
    assert table_nopk in list(cat2)
    d2 = cat.tables.temp.read()
    assert df.equals(d2)
    d_noindex = getattr(cat.tables, table_nopk).read()
    assert df2.equals(d_noindex)


@pytest.fixture()
def remote_sql(temp_db):
    pytest.importorskip("tornado")
    pytest.importorskip("msgpack")
    pytest.importorskip("requests")
    port = random.randint(1025, 64000)
    table, table_nopk, uri = temp_db
    os.environ['TEST_SQLITE_URI'] = uri  # used in catalog default
    fn = os.path.join(here, 'cat.yaml')
    cmd = ["intake-server", fn, "-p", f"{port}"]
    proc = subprocess.Popen(cmd, env=os.environ)
    timeout = 5
    while True:
        try:
            if requests.get(f"http://localhost:{port}/v1/info").ok:
                break
        except:
            pass
        time.sleep(0.1)
        timeout -= 0.1
        assert timeout > 0, "Server did not come up"
    yield f"intake://localhost:{port}"
    proc.terminate()
    proc.wait()


def test_with_server(remote_sql):
    cat = intake.open_catalog(remote_sql)
    s = cat.tables.temp()
    df = s.read()
    assert df.shape == (100, 4)
