import pandas as pd
import numpy as np
import os
import pytest
import tempfile
import sqlalchemy as sa


df = pd.DataFrame({
    'a': np.random.rand(100).tolist(),
    'b': np.random.randint(100).tolist(),
    'c': np.random.choice(['a', 'b', 'c', 'd'], size=100).tolist()
})
df.index.name = 'p'
df2 = pd.DataFrame({
    'd': np.random.rand(100).tolist(),
    'e': np.random.randint(100).tolist(),
    'f': np.random.choice(['a', 'b', 'c', 'd'], size=100).tolist()
})


@pytest.fixture(scope='module')
def temp_db():
    f = tempfile.mkstemp(suffix='.db')[1]
    uri = 'sqlite:///' + f
    engine = sa.create_engine(uri)
    con = engine.connect()
    con.execute(
        """CREATE TABLE temp (
        p BIGINT PRIMARY KEY,
        a REAL NOT NULL,
        b BIGINT NOT NULL,
        c TEXT NOT NULL);""")
    con.execute(
        """CREATE TABLE temp2 (
        d REAL NOT NULL,
        e BIGINT NOT NULL,
        f TEXT NOT NULL);""")
    df.to_sql('temp', uri, if_exists='append')
    df2.to_sql('temp_nopk', uri, if_exists='append', index=False)
    try:
        yield 'temp', 'temp_nopk', uri
    finally:
        if os.path.isfile(f):
            os.remove(f)
