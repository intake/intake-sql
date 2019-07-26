
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

    def __init__(self, uri, views=False, **kwargs):
        self.uri = uri
        self.views = views
        super(SQLCatalog, self).__init__(**kwargs)

    def _load(self):
        import sqlalchemy
        from intake_sql import SQLSourceAutoPartition
        engine = sqlalchemy.create_engine(self.uri)
        meta = sqlalchemy.MetaData(bind=engine)
        meta.reflect(views=self.views)
        self._entries = {}
        for name, table in meta.tables.items():
            for c in table.columns:
                if c.primary_key:
                    description = 'SQL table %s from %s' % (name, self.uri)
                    args = {'uri': self.uri, 'table': name, 'index': c.name,
                            'sql_kwargs': {}}
                    e = LocalCatalogEntry(name, description, 'sql_auto', True,
                                          args, {}, {}, {}, "", getenv=False,
                                          getshell=False)
                    e._plugin = [SQLSourceAutoPartition]
                    self._entries[name] = e
                    break
