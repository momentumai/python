import time
from math import floor


def to_five_minutes(timestamp):
    return floor(float(timestamp) / (5 * 60)) * 5 * 60


def to_hour(timestamp):
    return floor(float(timestamp) / (60 * 60)) * 60 * 60


def get_five_minutes_prev():
    return int(to_five_minutes(time.time()) - 5 * 60)
