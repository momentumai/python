from google.appengine.ext import ndb

class HistoryReferrerModel(ndb.Model):
    value = ndb.StringProperty(required=True, indexed=False)

class HistoryCampaignModel(ndb.Model):
    value = ndb.StringProperty(required=True, indexed=False)

class RealtimeContentModel(ndb.Model):
    value = ndb.StringProperty(required=True, indexed=False)

class RealtimeContentModelV2(ndb.Model):
    value = ndb.StringProperty(required=True, indexed=False)

class RealtimeDashboardModel(ndb.Model):
    value = ndb.StringProperty(required=True, indexed=False)
