from flask import current_app as app
import MySQLdb
import os


def get_service():
    connection = {}

    if app.config.get('DB_SOCKET', None) is not None:
        connection['unix_socket'] = app.config['DB_SOCKET']
    else:
        connection['host'] = app.config['DB_HOST']

    connection['user'] = app.config['DB_USER']
    connection['passwd'] = app.config['DB_PASSWORD']
    connection['db'] = app.config['DB_DATABASE']

    return MySQLdb.connect(**connection)


def fetch(file_path, query, params, db):
    query_string = open(os.path.join(
        os.path.dirname(file_path),
        'sql',
        '{}.sql'.format(query)
    ), 'r').read().format(**params)

    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(query_string)

    return cursor


def iter(cursor, size=10):
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row
