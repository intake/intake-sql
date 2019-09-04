
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

import intake
from .intake_sql import (SQLSource, SQLSourceAutoPartition,
                         SQLSourceManualPartition)
from .sql_cat import SQLCatalog
