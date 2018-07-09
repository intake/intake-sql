
from intake import Catalog
from .intake_sql import SQLSourceAutoPartition


class SQLCatalog(Catalog):
    """
    Makes data sources out of known tables in the given SQL service
    """

    def __init__(self, uri, views=False, **kwargs):
        self.uri = uri
        self.views = views
        self.name = kwargs.get('name', None)
        self.ttl = kwargs.get('ttl', 1)
        self.getenv = kwargs.pop('getenv', True)
        self.getshell = kwargs.pop('getshell', True)
        self.auth = kwargs.pop('auth', None)
        self.kwargs = kwargs
        self.metadata = {}
        self._entries = []
        self.reload()

    def reload(self):
        import sqlalchemy
        engine = sqlalchemy.create_engine(self.uri)
        meta = sqlalchemy.MetaData(bind=engine)
        meta.reflect(views=self.views)
        self._entries = []
        self._all_entries = {}
        for name, table in meta.tables.items():
            for c in table.columns:
                if c.primary_key:
                    e = SQLSourceAutoPartition(self.uri, name, c.name,
                                               self.kwargs)
                    self._entries.append(e)
                    self._all_entries[name] = e
                    break
        self._entry_tree = self._all_entries

    @property
    def changed(self):
        return False
