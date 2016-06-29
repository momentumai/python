from google.appengine.ext import ndb


def incremental_update(models, function):
    results = []
    get_keys = []
    for model in models:
        get_keys.append(model.key)

    old_models = ndb.get_multi(get_keys)
    for index in xrange(0, len(models)):
        results.append(function(old_models[index], models[index]))

    ndb.put_multi(results)
