from intake.source import base
from dask import dataframe as dd
import pandas as pd
import sqlalchemy as sa


class Plugin(base.Plugin):
    def __init__(self):
        super(Plugin, self).__init__(name='postgres', version='0.1', container='dataframe', partition_access=True)

    def open(self, uri, sql_expr, **kwargs):
        """
        Parameters:
            uri : str
                Full SQLAlchemy URI for the database connection.
            sql_expr : string or SQLAlchemy Selectable (select or text object):
                Table name (if string); otherwise, SQLAlchemy query to be executed.
            kwargs (dict):
                Additional parameters to pass to ``dask.dataframe.from_sql_table()``.
                Can nest another ``kwargs`` into it, to pass parameters to ``pandas.read_sql_table()``.
        """
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return PostgresSource(uri=uri, sql_expr=sql_expr, pg_kwargs=source_kwargs, metadata=base_kwargs['metadata'])


class PostgresSource(base.DataSource):
    def __init__(self, uri, sql_expr, pg_kwargs, metadata):
        self._init_args = dict(uri=uri, sql_expr=sql_expr, pg_kwargs=pg_kwargs, metadata=metadata)

        self._uri = uri
        self._sql_expr = sql_expr
        self._pg_kwargs = pg_kwargs
        self._dataframe = None

        super(PostgresSource, self).__init__(container='dataframe', metadata=metadata)

    def _get_dataframe(self):
        if self._dataframe is None:
            # index_col = pg_kwargs.get('index_col', 'index')
            # self._dataframe = dd.read_sql_table(self._sql_expr, self._uri, index_col, **self._pg_kwargs)
            engine = sa.create_engine(self._uri)
            self._dataframe = pd.read_sql_query(self._sql_expr, engine)

            dtypes = self._dataframe.dtypes
            self.datashape = None
            self.dtype = list(zip(dtypes.index, dtypes))
            self.shape = (len(self._dataframe),)
            self.npartitions = 1 # self._dataframe.npartitions

        return self._dataframe

    def discover(self):
        self._get_dataframe()
        return dict(datashape=self.datashape, dtype=self.dtype, shape=self.shape, npartitions=self.npartitions)

    def read(self):
        return self._get_dataframe() # .compute()

    # def read_chunked(self):
    #     df = self._get_dataframe()
    #
    #     for i in range(df.npartitions):
    #         yield df.get_partition(i).compute()
    #
    # def read_partition(self, i):
    #     df = self._get_dataframe()
    #     return df.get_partition(i).compute()
    #
    # def to_dask(self):
    #     return self._get_dataframe()

    def close(self):
        self._dataframe = None

    def __getstate__(self):
        return self._init_args

    def __setstate__(self, state):
        self.__init__(**state)
