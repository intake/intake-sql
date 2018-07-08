from intake.source import base
import pandas as pd


class SQLSource(base.DataSource):
    """
    One-shot SQL to dataframe reader (no partitioning)

    Caches entire dataframe in memory.

    Parameters
    ----------
    uri: str or None
        Full connection string in sqlalchemy syntax
    sql_expr: str
        Query expression to pass to the DB backend
    sql_kwargs: dict
        Further arguments to pass to pandas.read_sql
    """

    def __init__(self, uri, sql_expr, sql_kwargs={}, metadata={}):
        self._init_args = {
            'uri': uri,
            'sql_expr': sql_expr,
            'sql_kwargs': sql_kwargs,
            'metadata': metadata,
        }

        self._uri = uri
        self._sql_expr = sql_expr
        self._sql_kwargs = sql_kwargs
        self._dataframe = None

        super(SQLSource, self).__init__(container='dataframe',
                                        metadata=metadata)

    def _load(self):
        self._dataframe = pd.read_sql(self._sql_expr, self._uri,
                                      **self._sql_kwargs)

    def _get_schema(self):
        if self._dataframe is None:
            self._load()
        return base.Schema(datashape=None,
                           dtype=self._dataframe.dtypes,
                           shape=self._dataframe.shape,
                           npartitions=1,
                           extra_metadata={})

    def _get_partition(self, _):
        if self._dataframe is None:
            self._load_metadata()
        return self._dataframe

    def _close(self):
        self._dataframe = None


class SQLSourceAutoPartition(base.DataSource):
    """
    SQL table reader with automatic partitioning

    Only reads existing tables, not arbitrary SQL expressions.

    For partitioning, require to provide the column to be used, which should
    be indexed in the database. Can then provide list of boundaries, number
    of partitions or target partition size; see dask.dataframe.read_sql_table
    and examples for a list of possibilities.

    Parameters
    ----------
    uri: str or None
        Full connection string in sqlalchemy syntax
    table: str
        Table to read
    index: str
        Column to use for partitioning and as the index of the resulting
        dataframe
    sql_kwargs: dict
        Further arguments to pass to dask.dataframe.read_sql
    """

    def __init__(self, uri, table, index, sql_kwargs={}, metadata={}):
        self._init_args = {
            'uri': uri,
            'sql_expr': table,
            'index': index,
            'sql_kwargs': sql_kwargs,
            'metadata': metadata,
        }

        self._uri = uri
        self._sql_expr = table
        self._sql_kwargs = sql_kwargs
        self._index = index
        self._dataframe = None

        super(SQLSourceAutoPartition, self).__init__(container='dataframe',
                                                     metadata=metadata)

    def _load(self):
        import dask.dataframe as dd
        self._dataframe = dd.read_sql_table(self._sql_expr, self._uri,
                                            self._index, **self._sql_kwargs)

    def _get_schema(self):
        if self._dataframe is None:
            self._load()
        return base.Schema(datashape=None,
                           dtype=self._dataframe,
                           shape=(None, len(self._dataframe.columns)),
                           npartitions=self._dataframe.npartitions,
                           extra_metadata={})

    def _get_partition(self, i):
        if self._dataframe is None:
            self._load_metadata()
        return self._dataframe.get_partition(i).compute()

    def to_dask(self):
        self._get_schema()
        return self._dataframe

    def read(self):
        self._get_schema()
        return self._dataframe.compute()

    def _close(self):
        self._dataframe = None


class SQLSourceManualPartition(base.DataSource):
    """
    SQL expression reader with explicit partitioning

    Reads any arbitrary SQL expressions into pa5titioned data-frame, but
    requires a full specification of the boundaries.

    The boundaries are specified as either a set of strings with `WHERE`
    clauses to be applied to the main SQL expression, or a string to be
    formatted with a set of values to produce the comlete SQL expressions.

    Note, if not supplying a `meta` argument, dask will load the first
    partition in order to determine the schema. If some of the partitions are
    empty, loading without a meta will likely fail.

    ## TODO: implement meta=True or similar to mean "get meta from first part"?

    Parameters
    ----------
    uri: str or None
        Full connection string in sqlalchemy syntax
    table: str
        Table to read
    where_values: list of str or list of values/tuples
    where_template: str (optional)

    sql_kwargs: dict
        Further arguments to pass to dask.dataframe.read_sql
    """

    def __init__(self, uri, sql_expr, where_values, where_template=None,
                 sql_kwargs={}, metadata={}):
        self._init_args = {
            'uri': uri,
            'sql_expr': sql_expr,
            'where': where_values,
            'where_tmp': where_template,
            'sql_kwargs': sql_kwargs,
            'metadata': metadata,
        }

        self._uri = uri
        self._sql_expr = sql_expr
        self._sql_kwargs = sql_kwargs
        self._where = where_values
        self._where_tmp = where_template
        self._dataframe = None
        self._meta = self._sql_kwargs.pop('meta', None)

        super(SQLSourceManualPartition, self).__init__(container='dataframe',
                                                       metadata=metadata)

    def _load(self):
        self._dataframe = read_sql_query(self._uri, self._sql_expr,
                                         self._where, where_tmp=self._where_tmp,
                                         meta=self._meta, **self._sql_kwargs)

    def _get_schema(self):
        if self._dataframe is None:
            self._load()
        return base.Schema(datashape=None,
                           dtype=self._dataframe,
                           shape=(None, len(self._dataframe.columns)),
                           npartitions=self._dataframe.npartitions,
                           extra_metadata={})

    def _get_partition(self, i):
        if self._dataframe is None:
            self._load_metadata()
        return self._dataframe.get_partition(i).compute()

    def to_dask(self):
        self._get_schema()
        return self._dataframe

    def read(self):
        self._get_schema()
        return self._dataframe.compute()

    def _close(self):
        self._dataframe = None


def load_part(sql, engine, where, kwargs, meta=None):
    sql = sql + ' ' + where
    df = pd.read_sql(sql, engine, **kwargs)
    if meta is not None:
        if df.empty:
            df = meta
        else:
            df = df.astype(meta.dtypes.to_dict(), copy=False)
    return df


def read_sql_query(uri, sql, where, where_tmp=None, meta=None, kwargs=None):
    import dask
    import dask.dataframe as dd
    if where_tmp is not None:
        where = [where_tmp.format(values) for values in where]
    if kwargs is None:
        kwargs = {}
    dload = dask.delayed(load_part)
    parts = [dload(sql, uri, w, kwargs) for w in where]
    return dd.from_delayed(parts, meta=meta)
