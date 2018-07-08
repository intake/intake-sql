# Intake-ODBC
[![Build Status](https://travis-ci.org/ContinuumIO/intake-odbc.svg?branch=master)](https://travis-ci.org/ContinuumIO/intake-odbc)
[![Documentation Status](https://readthedocs.org/projects/intake-odbc/badge/?version=latest)](http://intake-odbc.readthedocs.io/en/latest/?badge=latest)

ODBC Plugin for Intake

Open Database Connectivity (ODBC) provides a standardised interface to
many data-base systems and other data sources capable of similar
functionality. It passes an SQL text query to the backend via a driver
and routes the resultant data back to the calling program.

ODBC can access data in several Data Source Names (DSNs), each of which
constitutes a specific connection to a specific database system of a
given type.

## User Installation

To install the intake-odbc plugin, execute the following command
```
conda install -c intake intake-odbc
```

In most cases, the end-users of ODBC data do not configure their
own DSNs, merely reference an existing system-wide configuration.
Each DSN will be referenced by a unique name, and in general
have different requirements in terms of further required parameters.
All of this may be defined in the Intake catalog, so that each
catalog entry executes a query on some ODBC backend without the user
having to know how this is achieved.

In order to set up the ODBC system on a machine, general guidance can
be found on the `turbodbc` pages (this is the library used by this
plugin): [odbc confguration](http://turbodbc.readthedocs.io/en/latest/pages/odbc_configuration.html),
[database configuration](http://turbodbc.readthedocs.io/en/latest/pages/databases.html).

## Developer Installation

- Create a development environment with `conda create`. Then install the dependencies:

```
conda install -c intake intake turbodbc pandas unixodbc
```

- Development installation:
```
git clone https://github.com/ContinuumIO/intake-odbc
cd intake-odbc
python setup.py develop --no-deps
```

- Create a DB to connect to, if you do not have an existing ODBC
    configuration. The module `tests/util.py` contains functions to
    start and stop MS SQL and PostgreSQL docker images that might be
    useful. You will generally need a driver, appropriate for your
    system and the DB backend you wish to connect to.

- Set up odbc config, e.g., the two ``.ini`` files provided. The
    default location of these files is system wide at ``/usr/local/etc``,
    but you can override this by specifying an environment variable
    before starting python, e.g.,

```
    export ODBCSYSINI=/path/to/intake-odbc/examples
```
