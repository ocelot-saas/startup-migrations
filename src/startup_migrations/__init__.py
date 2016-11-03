import errno
import fcntl
import os
import tempfile
import time

from yoyo import read_migrations, get_backend
import psycopg2


def migrate(database_url, migrations_path):
    """Migrate the database at database_url with scripts from migrations_path.

    Can be ran inside a pool of worker processes and a single one will do the actual
    migration."""

    try:
        _allow_only_one_process()
    except IOError as e:
        return

    retries = 0
    while retries < 10:
        try:
            backend = get_backend(database_url)
            break
        except psycopg2.OperationalError:
            retries += 1
            time.sleep(1.0)
    else:
        raise Exception('Could not connect to the database')
    
    migrations = read_migrations(migrations_path)
    backend.apply_migrations(backend.to_apply(migrations))


def _allow_only_one_process():
    lock_file_path = os.path.join(
        tempfile.gettempdir(), 'migrations.lock.{}'.format(os.getppid()))
    
    with open(lock_file_path, 'w') as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError as e:
            raise IOError('Somebody already doing the migration') from e
