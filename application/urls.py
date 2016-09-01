"""
urls.py

URL dispatch route mappings and error handlers

"""
import os

from flask import request

from application.views.public.public_warmup import public_warmup
from application.views.realtime.dashboard import realtime_dashboard
from application.views.realtime.content import realtime_content
from application.views.history.referrer import history_referrer
from application.views.history.campaign import history_campaign
from application.views.management import copy_old_data
from application.views.rule.schedule import schedule


def filter_ip():
    if (not os.environ['SERVER_SOFTWARE'].startswith('Dev') and
            request.remote_addr != '0.1.0.2'):
        return 'Unauthorized', 401

def add_rules(app):

    app.add_url_rule(
        '/_ah/warmup',
        'public_warmup',
        view_func=public_warmup
    )

    app.add_url_rule(
        '/realtime/dashboard/<int:level>',
        'realtime_dashboard',
        view_func=realtime_dashboard,
        methods=['GET']
    )

    app.add_url_rule(
        '/realtime/content/<int:level>',
        'realtime_content',
        view_func=realtime_content,
        methods=['GET']
    )

    app.add_url_rule(
        '/history/referrer',
        'history_referrer',
        view_func=history_referrer,
        methods=['GET']
    )

    app.add_url_rule(
        '/history/campaign',
        'history_campaign',
        view_func=history_campaign,
        methods=['GET']
    )

    app.add_url_rule(
        '/management/copy_old_data',
        'copy_old_data',
        view_func=copy_old_data,
        methods=['GET']
    )

    app.add_url_rule(
        '/rule/schedule',
        'schedule_rule',
        view_func=schedule,
        methods=['GET']
    )

    # app.before_request(filter_ip)
