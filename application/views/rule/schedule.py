# -*- coding: utf-8 -*-

from application.resources import mysql
from google.appengine.api import taskqueue
from application.utils import custom_time
from flask import current_app as app
import time
import json


def schedule():
    db = mysql.get_service()
    timestamp = custom_time.to_minutes(5, 10, time.time())
    futures = []

    result = mysql.fetch(__file__, 'get_rules', {'exclude': 5}, db)

    for row in mysql.iter(result, 1):
        payload = {
            'access_token': app.config['WORKER_TOKEN'],
            'team_id': str(row['team_id']),
            'timestamp': timestamp,
            'rules': [row]
        }
        headers = {
            'Content-Type': 'application/json'
        }
        task = taskqueue.Task(
            json.dumps(payload),
            url='/api/worker/rules',
            headers=headers)
        future = taskqueue.Queue(name='rule').add_async(task)
        futures.append(future)

    db.close()

    for future in futures:
        future.get_result()

    return 'Success'
