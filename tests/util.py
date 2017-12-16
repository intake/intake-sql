import os
import shlex
import subprocess


def verify_plugin_interface(plugin):
    assert isinstance(plugin.version, str)
    assert isinstance(plugin.container, str)
    assert isinstance(plugin.partition_access, bool)


def verify_datasource_interface(source):
    for attr in ['container', 'description', 'datashape', 'dtype', 'shape',
                 'npartitions', 'metadata']:
        assert hasattr(source, attr)

    for method in ['discover', 'read', 'read_chunked', 'read_partition',
                   'to_dask', 'close']:
        assert hasattr(source, method)


def start_postgres():
    print('Starting PostgreSQL server...')

    cmd = shlex.split('docker run --name postgres-db --publish 5432:5432 mdillon/postgis:9.6-alpine')
    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            universal_newlines=True)

    pg_init = False
    while True:
        output_line = proc.stdout.readline()
        print(output_line.rstrip())
        if proc.poll() is not None: # If the process exited
            raise Exception('PostgreSQL server failed to start up properly.')
        if 'PostgreSQL init process complete; ready for start up' in output_line:
            pg_init = True
        elif pg_init and 'database system is ready to accept connections' in output_line:
            break


def stop_postgres(let_fail=False):
    try:
        print('Stopping PostgreSQL server...')
        subprocess.check_call('docker ps -q --filter "name=postgres-db" | xargs docker rm -vf', shell=True)
    except subprocess.CalledProcessError:
        if not let_fail:
            raise
