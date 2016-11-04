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
    migration.
    """

    lock_file = _acquire_lock()

    if lock_file is None:
        return

    try:
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
    finally:
        _release_lock(lock_file)


def _acquire_lock():
    lock_file_path = os.path.join(
        tempfile.gettempdir(), 'migrations.lock.{}'.format(os.getppid()))
    lock_file = open(lock_file_path, 'w')

    other_got_lock = False

    # The following loop is quite complicated. It will either immediatly acquire a lock
    # at the file system level by locking $lock_file and return it to the caller. Or, if
    # it does not manage to be the first to acquire the lock, will wait until the first
    # process does its job and releases the lock, and then return None to the callers,
    # forcing them to stop.
    while True:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if not other_got_lock:
                # Managed to get the lock from the get go
                return lock_file
            else:
                # Did not get the lock from the get go, so return null.
                _release_lock(lock_file)
                return None
        except IOError as e:
            if e.errno == errno.EAGAIN:
                # Could not get lock because another process got it. Will wait until
                # the lock is release / the other process has finished, then will
                # return null.
                other_got_lock = True
                time.sleep(1)
            else:
                raise e


def _release_lock(lock_file):
    fcntl.flock(lock_file, fcntl.LOCK_UN)
    lock_file.close()
