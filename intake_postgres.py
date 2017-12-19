from intake.source import base
from dask import dataframe as dd


class Plugin(base.Plugin):
    def __init__(self):
        super(Plugin, self).__init__(name='postgres', version='0.1', container='dataframe', partition_access=True)

    def open(self, uri, sql_expr, index_col, **kwargs):
        """
        Parameters:
            uri : str
                Full SQLAlchemy URI for the database connection.
            sql_expr : string or SQLAlchemy Selectable (select or text object):
                Table name (if string); otherwise, SQLAlchemy query to be executed.
            index_col (string):
                Column which becomes the index, and defines the
                partitioning. Should be an indexed column in the SQL server, and
                numerical. Could be a function to return a value. Labeling
                columns created by functions or arithmetic operations is
                required. Example:
                ``sql.func.abs(sql.column('value')).label('abs(value)')``
            kwargs (dict):
                Additional parameters to pass to ``dask.dataframe.from_sql_table()``.
                Can nest another ``kwargs`` into it, to pass parameters to ``pandas.read_sql_table()``.
        """
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return PostgresSource(uri=uri, sql_expr=sql_expr, index_col=index_col, pg_kwargs=source_kwargs, metadata=base_kwargs['metadata'])


class PostgresSource(base.DataSource):
    def __init__(self, uri, sql_expr, index_col, pg_kwargs, metadata):
        self._init_args = dict(uri=uri, sql_expr=sql_expr, index_col=index_col, pg_kwargs=pg_kwargs, metadata=metadata)

        self._uri = uri
        self._sql_expr = sql_expr
        self._index_col = index_col
        self._pg_kwargs = pg_kwargs
        self._dataframe = None

        super(PostgresSource, self).__init__(container='dataframe', metadata=metadata)

    def _get_dataframe(self):
        if self._dataframe is None:
            self._dataframe = dd.read_sql_table(self._sql_expr, self._uri, self._index_col, **self._pg_kwargs)

            dtypes = self._dataframe.dtypes
            self.datashape = None
            self.dtype = list(zip(dtypes.index, dtypes))
            self.shape = (len(self._dataframe),)
            self.npartitions = self._dataframe.npartitions

        return self._dataframe

    def discover(self):
        self._get_dataframe()
        return dict(datashape=self.datashape, dtype=self.dtype, shape=self.shape, npartitions=self.npartitions)

    def read(self):
        return self._get_dataframe().compute()

    def read_chunked(self):
        df = self._get_dataframe()

        for i in range(df.npartitions):
            yield df.get_partition(i).compute()

    def read_partition(self, i):
        df = self._get_dataframe()
        return df.get_partition(i).compute()

    def to_dask(self):
        return self._get_dataframe()

    def close(self):
        self._dataframe = None

    def __getstate__(self):
        return self._init_args

    def __setstate__(self, state):
        self.__init__(**state)
