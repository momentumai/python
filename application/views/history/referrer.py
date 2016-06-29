# -*- coding: utf-8 -*-

import json
from google.appengine.ext import ndb
from application.resources import bigquery
from application.utils.datastore import incremental_update
from application.models.datastore import HistoryReferrerModel
from application.utils.file import read_file
from application.utils.custom_time import get_five_minutes_prev
from application.utils.bigquery import (
    to_datastore_models,
    format_id,
    inverse_group_concat,
    get_table_name_hour
)


def format_model(line, _):
    model_id = [
        line['team_id'],
        line['content_id']
    ]

    by_cat = inverse_group_concat(
        line['referrer_list'],
        ['referrer'],
        '||||',
        '|||'
    )
    model_value = {}

    for key, value in by_cat.iteritems():
        model_value[key] = {}
        model_value[key]['referrer'] = inverse_group_concat(
            value['referrer'],
            ['shares'],
            '||',
            '|'
        )

    return {
        'key': ndb.Key(HistoryReferrerModel, format_id(model_id)),
        'value': str(json.dumps(model_value))
    }


def increase(model_old, model):
    if model_old is None:
        model_old = {
            'value': '{}'
        }
    else:
        model_old = model_old.to_dict()

    old = json.loads(model_old.get('value', '{}'))
    cur = json.loads(model.value)

    for cur_main_key, cur_main_val in cur.iteritems():
        old_main_val = old.get(cur_main_key, {})
        old[cur_main_key] = old_main_val

        old_ref = old_main_val.get('referrer', {})
        old[cur_main_key]['referrer'] = old_ref
        cur_ref = cur_main_val.get('referrer', {})

        for cur_sub_key, cur_sub_val in cur_ref.iteritems():
            old_referrer = old[cur_main_key]['referrer']
            old_sub_val = old_ref.get(cur_sub_key, {})
            old_referrer[cur_sub_key] = old_sub_val

            old_share = int(old_sub_val.get('shares', 0))
            cur_share = int(cur_sub_val.get('shares', 0))
            old_referrer[cur_sub_key]['shares'] = old_share + cur_share

    model.value = str(json.dumps(old))
    return model


def response_handler(schema, rows, params):
    models = to_datastore_models(
        schema,
        rows,
        HistoryReferrerModel,
        format_model,
        params
    )

    incremental_update(models, increase)


def history_referrer():

    query = read_file(__file__, '/bigquery/referrer-query.sql')

    query_time = get_five_minutes_prev()
    query_table = get_table_name_hour(query_time)

    bigquery.query(query, None, response_handler, {
        'timeout': 20,
        'max_result': 250,
        'service': {
            'maximumBillingTier': 2
        }
    }, {
        'time': query_time,
        'table': query_table
    })

    return "Succeed"
