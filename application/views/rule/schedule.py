# -*- coding: utf-8 -*-

from application.resources import mysql
from google.appengine.api import taskqueue


def schedule():
    db = mysql.get_service()
    features = []

    result = mysql.fetch(__file__, 'get_rules', {'exclude': 5}, db)

    for row in mysql.iter(result, 1):
        task = taskqueue.Task(None, url='/rule/run', params=row)
        feature = taskqueue.Queue(name='rule').add_async(task)
        features.append(feature)

    db.close()

    for feature in features:
        feature.get_result()

    return 'Success'
