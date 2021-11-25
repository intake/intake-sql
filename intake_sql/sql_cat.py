from . import __version__
from collections.abc import Mapping
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
        engine = sqlalchemy.create_engine(self.uri)
        meta = sqlalchemy.MetaData(bind=engine)
        meta.reflect(views=self.views, schema=self.sql_kwargs.get("schema"))
        self._entries = SQLEntries(meta, self.uri, self.sql_kwargs)


class SQLEntries(Mapping):

    def __init__(self, meta, uri, sql_kwargs):
        self.meta = meta
        self.uri = uri
        self.sql_kwargs = sql_kwargs
        self.tables = None
        self.cache = {}

    def _get_tables(self):
        if self.tables is None:
            self.tables = list(self.meta.tables)

    def _make_entry(self, name):
        if name in self.cache:
            return
        from intake_sql import SQLSource, SQLSourceAutoPartition
        description = 'SQL table %s from %s' % (name, self.uri)
        table = self.meta.tables[name]
        for c in table.columns:
            # We use table.name instead of the metadata key here as it
            # does not include the schema name, which is handled
            # by the `sql_kwargs`.
            if c.primary_key:

                description = 'SQL table %s from %s' % (name, self.uri)
                args = {'uri': self.uri, 'table': table.name, 'index': c.name,
                        'sql_kwargs': self.sql_kwargs}
                e = LocalCatalogEntry(table.name, description, 'sql_auto', True,
                                      args, {}, [], {}, "", getenv=False,
                                      getshell=False)
                e._plugin = [SQLSourceAutoPartition]
                self.cache[name] = e
                break
        else:
            args = {
                'uri': self.uri,
                'sql_expr': table.name,
                'sql_kwargs': self.sql_kwargs
            }
            e = LocalCatalogEntry(name,description, 'sql', True,
                                  args, {}, [], {}, "", getenv=False,
                                  getshell=False)
            e._plugin = [SQLSource]
            self.cache[name] = e

    def keys(self):
        self._get_tables()
        return self.tables

    def __getitem__(self, item):
        self._make_entry(item)
        return self.cache[item]

    def __len__(self):
        return len(self.keys())

    def __iter__(self):
        return iter(self.keys())

