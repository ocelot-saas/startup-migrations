import time

from yoyo import read_migrations, get_backend
import psycopg2


def migrate(database_url, migrations_path):
    """Migrate the database at database_url with scripts from migrations_path."""

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
