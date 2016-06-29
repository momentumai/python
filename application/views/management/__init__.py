import time
from datetime import datetime, date
from flask import current_app as app
from application.resources import bigquery


def copy_old_data():
    # worker interval x hours
    interval = 1

    # what => where
    copy_vector = {}

    now = time.time()

    # round down to hour
    now -= now % 3600

    # minus 36 hours
    now -= 36 * 3600

    while interval != 0:
        i = now - interval * 3600
        copy_vector[
            datetime.fromtimestamp(i).strftime('%Y%m%d%H')
        ] = date.fromtimestamp(i).strftime('%Y%m%d')
        interval -= 1

    for what, where in copy_vector.iteritems():
        bigquery.copy(
            'views_' + what,
            'views_' + where,
            app.config['BIGQUERY_DATASET_REALTIME'],
            app.config['BIGQUERY_DATASET_ANALYTICS'],
            {}
        )

    return 'Succeed'
