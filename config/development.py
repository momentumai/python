from config.base import Base


class Development(Base):
    DEBUG = True
    DEBUG_TB_PROFILER_ENABLED = True
    DEBUG_TB_INTERCEPT_REDIRECTS = False
    CSRF_ENABLED = True
    SECRET_KEY = 'nAIjU8S6tApXxc6X4SHw6aAi'
    CSRF_SESSION_KEY = '2QjI1JcqEUo61P7ticA4NQTv'

    BIGQUERY_DRYRUN = False
    BIGQUERY_DATASET_REALTIME = 'dev_realtime'
    BIGQUERY_DATASET_ANALYTICS = 'dev_analytics'
    BIGQUERY_DATASET_REALTIME_RESULT = 'dev_realtime_result'

    DB_HOST = ''
    DB_USER = ''
    DB_PASSWORD = ''
    DB_DATABASE = ''

    WORKER_TOKEN = 'ULTRA-SECRET-TOKEN'
