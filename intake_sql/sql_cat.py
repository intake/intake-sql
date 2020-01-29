
from . import __version__
from intake.catalog.base import Catalog
from intake.catalog.local import LocalCatalogEntry


class SQLCatalog(Catalog):
    """
    Makes data sources out of known tables in the given SQL service

    This uses SQLAlchemy to infer the tables and views on the target server.
    Of these, those which have at least one primary key column will become
    ``SQLSourceAutoPartition`` entries in this catalog.
    """
    name = 'sql_cat'
    version = __version__

    def __init__(self, uri, views=False, sql_kwargs=None, **kwargs):
        self.sql_kwargs = sql_kwargs or {}
        self.uri = uri
        self.views = views
        super(SQLCatalog, self).__init__(**kwargs)

    def _load(self):
        import sqlalchemy
        from intake_sql import SQLSource, SQLSourceAutoPartition
        engine = sqlalchemy.create_engine(self.uri)
        meta = sqlalchemy.MetaData(bind=engine)
        meta.reflect(views=self.views, schema=self.sql_kwargs.get("schema"))
        self._entries = {}
        for name, table in meta.tables.items():
            description = 'SQL table %s from %s' % (name, self.uri)
            for c in table.columns:
                # We use table.name instead of the metadata key here as it
                # does not include the schema name, which is handled
                # by the `sql_kwargs`.
                if c.primary_key:
                    args = {'uri': self.uri, 'table': table.name, 'index': c.name,
                            'sql_kwargs': self.sql_kwargs}
                    e = LocalCatalogEntry(table.name, description, 'sql_auto', True,
                                          args, {}, {}, {}, "", getenv=False,
                                          getshell=False)
                    e._plugin = [SQLSourceAutoPartition]
                    self._entries[table.name] = e
                    break
            else:
                args = {
                    'uri': self.uri,
                    'sql_expr': table.name,
                    'sql_kwargs': self.sql_kwargs
                }
                e = LocalCatalogEntry(name,description, 'sql', True,
                                      args, {}, {}, {}, "", getenv=False,
                                      getshell=False)
                e._plugin = [SQLSource]
                self._entries[table.name] = e