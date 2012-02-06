import updates_index_sqlite
import updates_index_postgres

def UpdatesIndexer(backend, database, **options):
    if backend == 'sqlite':
        return updates_index_sqlite.UpdatesIndexer(database, **options)
    elif backend == 'postgres':
        return updates_index_postgres.UpdatesIndexer(database, **options)
    else:
        raise ValueError('Invalid database backend')

def UpdatesReader(backend, database, **options):
    if backend == 'sqlite':
        return updates_index_sqlite.UpdatesReader(database, **options)
    elif backend == 'postgres':
        return updates_index_postgres.UpdatesReader(database, **options)
    else:
        raise ValueError('Invalid database backend')
