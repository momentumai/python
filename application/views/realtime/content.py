# -*- coding: utf-8 -*-

import json
from google.appengine.ext import ndb
from flask import current_app as app
from application.resources import bigquery
from application.utils.file import read_file
from application.utils.custom_time import get_five_minutes_prev
from application.models.datastore import RealtimeContentModel
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
        params['from'],
        line['rank'],
        line['content_id']
    ]
    model_value = {
        'rank': str(line['rank']),
        'content_id': line['content_id'],
        'share': str(line['share']),
        'view': str(line['view']),
        'user': str(line['user']),
        'organic': str(line['organic']),
        'team': str(line['team']),
        'paid': str(line['paid']),
        'by_time': str(line['by_time']),
        'source': str(line['source'])
    }

    return {
        'key': ndb.Key(RealtimeContentModel, format_id(model_id)),
        'value': json.dumps(model_value)
    }


def response_handler(schema, rows, params):
    models = to_datastore_models(
        schema,
        rows,
        RealtimeContentModel,
        format_model,
        params
    )
    response = ndb.put_multi(models)
    app.logger.info('Datastore Response: {}'.format(response))


def realtime_content():
    udf = [{
        'inlineCode': read_file(__file__, '/bigquery/content-udf.js')
    }]

    query = read_file(__file__, '/bigquery/content-query.sql')

    from_time = get_five_minutes_prev()
    to_time = from_time - 24 * 60 * 60

    from_time_table = get_table_name_hour(from_time)
    to_time_table = get_table_name_hour(to_time)

    bigquery.query(query, udf, response_handler, {
        'timeout': 30,
        'max_result': 250,
        'service': {
            'maximumBillingTier': 2
        }
    }, {
        'dataset': app.config['BIGQUERY_DATASET_REALTIME'],
        'from_table': from_time_table,
        'to_table': to_time_table,
        'from': from_time,
        'to': to_time
    })

    return "Succeed"
