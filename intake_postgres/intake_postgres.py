from __future__ import absolute_import
from intake.source import base
import pandas as pd
from postgresadapter import PostgresAdapter
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class PostgresSource(base.DataSource):
    """Read data from PostgreSQL to dataframes

    Parameters
    ----------
    uri: str
        Connection to PostgreSQL server
    sql_expr: str
        The full text of the SQL query to execute
    pg_kwargs: dict
        Further args passed to postgresadapter.PostgresAdapter, see
        https://github.com/ContinuumIO/PostgresAdapter/blob/master/postgresadapter/core/PostgresAdapter.pyx#L281
    """
    name = 'postgres'
    container = 'dataframe'
    version = __version__
    partition_access = False

    def __init__(self, uri, sql_expr, pg_kwargs={}, metadata=None):
        self._uri = uri
        self._sql_expr = sql_expr
        self._pg_kwargs = pg_kwargs
        self._dataframe = None
        super(PostgresSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        if self._dataframe is None:
            # This approach is not optimal; LIMIT is know to confuse the query
            # planner sometimes. If there is a faster approach to gleaning
            # dtypes from arbitrary SQL queries, we should use it instead.
            first_rows = PostgresAdapter(
                self._uri,
                dataframe=True,
                query=('({}) limit 10').format(self._sql_expr),
                **self._pg_kwargs
            )._to_dataframe()
            dtype = first_rows[:0]
            shape = (None, len(first_rows.dtypes.index))
        else:
            dtype = self._dataframe[:0]
            shape = self._dataframe.shape
        dtype = {k: str(v) for k, v
                 in dtype.dtypes.to_dict().items()}
        return base.Schema(datashape=None,
                           dtype=dtype,
                           shape=shape,
                           npartitions=1,
                           extra_metadata={})

    def _get_partition(self, _):
        if self._dataframe is None:
            part = PostgresAdapter(
                self._uri,
                query=self._sql_expr,
                **self._pg_kwargs
            )
            _arr = part._to_array()
            self._dataframe = pd.DataFrame()
            for colname in part.field_names:
                col = _arr[colname]
                ncols = col.shape[1] if len(col.shape) > 1 else 1
                if ncols > 1:
                    for colct in range(ncols):
                        self._dataframe[colname+str(colct)] = col[:, colct]
                else:
                    self._dataframe[colname] = col
            # The schema should be corrected once the data is read.
            self._schema = None
        return self._dataframe

    def _close(self):
        self._dataframe = None
