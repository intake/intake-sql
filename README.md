# intake-postgres

[![Build Status](https://travis-ci.org/ContinuumIO/intake-postgres.svg?branch=master)](https://travis-ci.org/ContinuumIO/intake-postgres)
[![Documentation Status](https://readthedocs.org/projects/intake-postgres/badge/?version=latest)](http://intake-postgres.readthedocs.io/en/latest/?badge=latest)

PostgreSQL Plugin for [Intake](https://github.com/ContinuumIO/intake)

## User Installation

*Note: the following command does not work yet, and the developer installation is recommended.*
```
conda install -c intake intake-postgres
```

## Developer Installation

1. Create a development environment with `conda create`. Then install the dependencies:

    ```
    conda install -c intake intake
    conda install -n root conda-build
    git clone https://github.com/ContinuumIO/PostgresAdapter.git
    conda build PostgresAdapter/buildscripts/condarecipe
    conda install --use-local postgresadapter
    conda install pandas psycopg2 sqlalchemy postgresql pytest shapely
    ```

1. Development installation:
    ```
    python setup.py develop --no-deps
    ```

1. Running tests:
    ```
    pytest -v tests
    ```
