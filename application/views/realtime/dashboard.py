# -*- coding: utf-8 -*-

import json
from google.appengine.ext import ndb
from flask import current_app as app
from application.resources import bigquery
from application.utils.file import read_file
from application.utils.custom_time import get_five_minutes_prev
from application.models.datastore import RealtimeDashboardModel
from application.utils.bigquery import (
    to_datastore_models,
    format_id,
    get_table_name_hour
)


def format_model(line, params):
    model_id = [
        line['team_id'],
        line['cat1'],
        line['cat2'],
        line['cat3'],
        str(params['from'])
    ]
    model_value = {
        'by_time': str(line['by_time']),
        'share': str(line['share']),
        'user': str(line['user']),
        'view': str(line['view'])
    }

    return {
        'key': ndb.Key(RealtimeDashboardModel, format_id(model_id)),
        'value': json.dumps(model_value)
    }


def response_handler(schema, rows, params):
    models = to_datastore_models(
        schema,
        rows,
        RealtimeDashboardModel,
        format_model,
        params
    )
    response = ndb.put_multi(models)
    app.logger.info('Datastore Response: {}'.format(response))


def realtime_dashboard(level):
    query = read_file(__file__, '/bigquery/dashboard-query.sql')

    from_time = get_five_minutes_prev()
    to_time = from_time - 24 * 60 * 60

    from_time_table = get_table_name_hour(from_time)
    to_time_table = get_table_name_hour(to_time)

    bigquery.query(query, None, response_handler, {
        'timeout': 30,
        'max_result': 250,
        'service': {
            'maximumBillingTier': 1,
            'allowLargeResults': True,
            'destinationTable': {
                'datasetId': app.config['BIGQUERY_DATASET_REALTIME_RESULT']
            }
        }
    }, {
        'dataset': app.config['BIGQUERY_DATASET_REALTIME'],
        'from_table': from_time_table,
        'to_table': to_time_table,
        'from': from_time,
        'to': to_time,
        'cat1': 'cat1' if level > 0 else '\'NONE\'',
        'cat2': 'cat2' if level > 1 else '\'NONE\'',
        'cat3': 'cat3' if level > 2 else '\'NONE\'',
        'new_cat': 'new_cat%s' % level
    })

    return "Succeed"
