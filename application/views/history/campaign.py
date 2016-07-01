# -*- coding: utf-8 -*-

import json
from flask import current_app as app
from google.appengine.ext import ndb
from application.resources import bigquery
from application.utils.datastore import incremental_update
from application.models.datastore import HistoryCampaignModel
from application.utils.file import read_file
from application.utils.custom_time import get_five_minutes_prev
from application.utils.bigquery import (
    to_datastore_models,
    format_id,
    get_table_name_hour
)


def format_model(line, _):
    model_id = [
        line['team_id'],
        line['campaign']
    ]

    model_value = {
        'share': line['share']
    }

    return {
        'key': ndb.Key(HistoryCampaignModel, format_id(model_id)),
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

    old_share = int(old.get('share', 0))
    old_share += int(cur.get('share', 0))

    old['share'] = old_share

    model.value = str(json.dumps(old))
    return model


def response_handler(schema, rows, params):
    models = to_datastore_models(
        schema,
        rows,
        HistoryCampaignModel,
        format_model,
        params
    )

    incremental_update(models, increase)


def history_campaign():

    query = read_file(__file__, '/bigquery/campaign-query.sql')

    query_time = get_five_minutes_prev()
    query_table = get_table_name_hour(query_time)

    bigquery.query(query, None, response_handler, {
        'timeout': 20,
        'max_result': 250,
        'service': {
            'maximumBillingTier': 2
        }
    }, {
        'dataset': app.config['BIGQUERY_DATASET_REALTIME'],
        'time': query_time,
        'table': query_table
    })

    return "Succeed"
