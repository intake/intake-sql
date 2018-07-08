from collections import OrderedDict
import numpy as np
import pandas as pd
import pytest
import shlex
import subprocess
import time
import turbodbc

N = 10000
df0 = pd.DataFrame(OrderedDict([('productname', np.random.choice(
    ['fridge', 'toaster', 'kettle', 'micro', 'mixer', 'oven'], size=N)),
                                ('price', np.random.rand(N) * 10),
                                ('productdescription', ["hi"] * N)]))
df0.index.name = 'productid'


def verify_plugin_interface(plugin):
    """Assert types of plugin attributes."""
    assert isinstance(plugin.version, str)
    assert isinstance(plugin.container, str)
    assert isinstance(plugin.partition_access, bool)


def verify_datasource_interface(source):
    """Assert presence of datasource attributes."""
    for attr in ['container', 'description', 'datashape', 'dtype', 'shape',
                 'npartitions', 'metadata']:
        assert hasattr(source, attr)

    for method in ['discover', 'read', 'read_chunked', 'read_partition',
                   'to_dask', 'close']:
        assert hasattr(source, method)


def start_mssql():
    print('Starting MS SQL Server...')

    cmd = shlex.split("docker run -e 'ACCEPT_EULA=Y' --name intake-mssql "
                      "-e 'SA_PASSWORD=yourStrong(!)Password' "
                      "-p 1433:1433 -d microsoft/mssql-server-linux:2017-CU3")
    cid = subprocess.check_output(cmd).strip().decode()

    time.sleep(10)

    cmd = shlex.split("docker exec {} /opt/mssql-tools/bin/sqlcmd"
                      " -S localhost -U sa -P 'yourStrong(!)Password' "
                      "-Q 'CREATE DATABASE test'".format(cid))
    subprocess.check_output(cmd)


def stop_docker(name, let_fail=False):
    """Stop docker container with given name tag

    Parameters
    ----------
    name: str
        name field which has been attached to the container we wish to remove
    let_fail: bool
        whether to raise an exception if the underlying commands return an
        error.
    """
    try:
        print('Stopping %s ...' % name)
        cmd = shlex.split('docker ps -q --filter "name=%s"' % name)
        cid = subprocess.check_output(cmd).strip().decode()
        if cid:
            subprocess.call(['docker', 'kill', cid])
            subprocess.call(['docker', 'rm', cid])
    except subprocess.CalledProcessError as e:
        print(e)
        if not let_fail:
            raise


def start_postgres():
    """Bring up a container running PostgreSQL with PostGIS. Pipe the output of
    the container process to stdout, until the database is ready to accept
    connections. This container may be stopped with ``stop_docker()``.
    """
    # copied from intake-postgres
    print('Starting PostgreSQL server...')

    # More options here: https://github.com/appropriate/docker-postgis
    cmd = shlex.split('docker run --rm --name intake-postgres '
                      '--publish 5432:5432 mdillon/postgis:9.4-alpine')
    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            universal_newlines=True)

    # The database may have been restarted in the container, so track whether
    # initialization happened or not.
    pg_init = False
    while True:
        output_line = proc.stdout.readline()
        print(output_line.rstrip())
        # If the process exited, raise exception.
        if proc.poll() is not None:
            raise Exception('PostgreSQL server failed to start up properly.')
        # Detect when initialization has happened, so we can stop waiting when
        # the database is accepting connections.
        if ('PostgreSQL init process complete; '
                'ready for start up') in output_line:
            pg_init = True
        elif (pg_init and
              'database system is ready to accept connections' in output_line):
            break


@pytest.fixture(scope='module')
def mssql():
    """Start docker container for MS SQL and cleanup connection afterward."""
    stop_docker('intake-mssql', let_fail=False)
    start_mssql()

    kwargs = dict(dsn="MSSQL", uid='sa', pwd='yourStrong(!)Password',
                  mssql=True)
    timeout = 5
    try:
        while True:
            try:
                conn = turbodbc.connect(**kwargs)
                break
            except Exception as e:
                print(e)
                time.sleep(0.2)
                timeout -= 0.2
                if timeout < 0:
                    raise
        curs = conn.cursor()
        curs.execute("""CREATE TABLE testtable
            (productid int PRIMARY KEY NOT NULL,
             productname varchar(25) NOT NULL,
             price float NULL,
             productdescription text NULL)""")
        for i, row in df0.iterrows():
            curs.execute(
                "INSERT testtable (productid, productname, price, "
                "                  productdescription) "
                "VALUES ({}, '{}', {}, '{}')".format(*([i] + row.tolist())))
        conn.commit()
        conn.close()
        yield kwargs
    finally:
        stop_docker('intake-mssql')


@pytest.fixture(scope='module')
def pg():
    """Start docker container for MS SQL and cleanup connection afterward."""
    stop_docker('intake-postgres', let_fail=False)
    start_postgres()

    kwargs = dict(dsn="PG")
    timeout = 5
    try:
        while True:
            try:
                conn = turbodbc.connect(**kwargs)
                break
            except Exception as e:
                print(e)
                time.sleep(0.2)
                timeout -= 0.2
                if timeout < 0:
                    raise
        curs = conn.cursor()
        curs.execute("""CREATE TABLE testtable
            (productid int PRIMARY KEY NOT NULL,
             productname varchar(25) NOT NULL,
             price float NULL,
             productdescription text NULL)""")
        for i, row in df0.iterrows():
            curs.execute(
                "INSERT INTO testtable "
                "VALUES ({}, '{}', {}, '{}')".format(*([i] + row.tolist())))
        conn.commit()
        conn.close()
        yield kwargs
    finally:
        stop_docker('intake-postgres')
