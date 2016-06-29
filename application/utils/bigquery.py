from datetime import datetime
from application.utils.custom_time import to_hour


def format_id(id_items):
    result = []
    for item in id_items:
        result.append(str(item).replace('||', '--'))
    return '||'.join(result)


def get_line(schema, row):
    line = {}

    for i in xrange(0, len(schema)):
        cell = row['f'][i]
        field = schema[i]
        line[field['name']] = cell['v']
    return line


def to_datastore_models(schema, rows, model, format_model, params):
    models = []
    for row in rows:
        line = get_line(schema, row)
        args = format_model(line, params)
        models.append(model(**args))
    return models


def inverse_group_concat(value, labels, separator1, separator2):
    result = {}
    items = value.split(separator1)
    for item in items:
        values = item.split(separator2)
        result[values[0]] = {}
        for index in xrange(1, len(values)):
            label = labels[index - 1]
            result[values[0]][label] = values[index]
    return result


def get_table_name_hour(timestamp):
    hour = to_hour(timestamp)
    return datetime.fromtimestamp(hour).strftime('%Y%m%d%H')
