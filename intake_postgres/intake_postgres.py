from intake.source import base
import pandas as pd
from postgresadapter import PostgresAdapter


class PostgresSource(base.DataSource):
    def __init__(self, uri, sql_expr, pg_kwargs, metadata):
        self._init_args = {
            'uri': uri,
            'sql_expr': sql_expr,
            'pg_kwargs': pg_kwargs,
            'metadata': metadata,
        }

        self._uri = uri
        self._sql_expr = sql_expr
        self._pg_kwargs = pg_kwargs
        self._dataframe = None

        super(PostgresSource, self).__init__(container='dataframe',
                                             metadata=metadata)

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
