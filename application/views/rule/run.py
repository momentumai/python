from flask import request


def run():
    data = dict(request.form)
    return 'Succeed'
