import shlex
import subprocess


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


def start_postgres():
    """Bring up a container running PostgreSQL with PostGIS. Pipe the output of
    the container process to stdout, until the database is ready to accept
    connections. This container may be stopped with ``stop_postgres()``.
    """
    print('Starting PostgreSQL server...')

    # More options here: https://github.com/appropriate/docker-postgis
    cmd = shlex.split('docker run --rm --name intake-postgres --publish 5432:5432 '
                      'mdillon/postgis:9.4-alpine')
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


def stop_postgres(let_fail=False):
    """Attempt to shut down the container started by ``start_postgres()``.
    Raise an exception if this operation fails, unless ``let_fail``
    evaluates to True.
    """
    try:
        print('Stopping PostgreSQL server...')
        subprocess.check_call('docker ps -q --filter "name=intake-postgres" | '
                              'xargs docker rm -vf', shell=True)
    except subprocess.CalledProcessError:
        if not let_fail:
            raise
