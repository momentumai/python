import uuid
import time

from flask import current_app as app

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from application.resources.google_request_builder import build_request


def get_service():
    return build('bigquery', 'v2', requestBuilder=build_request)


def insert_query_job(service, query_string, udf, settings):
    job_id = str(uuid.uuid4())
    job_body = {
        'jobReference': {
            'jobId': job_id
        },
        'configuration': {
            'query': {
                'query': query_string,
                'dryRun': app.config['BIGQUERY_DRYRUN'],
                'maximumBillingTier': settings.get('maximumBillingTier', 1),
                'allowLargeResults': settings.get('allowLargeResults', False)
            }
        }
    }

    if settings.get('destinationTable', None):
        dest_table = {
            'projectId': app.config['PROJECT_ID'],
            'tableId': job_id.replace('-', '_'),
            'datasetId': settings['destinationTable']['datasetId']
        }
        job_body['configuration']['query']['destinationTable'] = dest_table

    if udf is not None:
        job_body['configuration']['query']['userDefinedFunctionResources'] = udf

    job = {
        'job_id': job_id,
        'start_time': time.time()
    }

    try:
        job['response'] = service.jobs().insert(
            projectId=app.config['PROJECT_ID'],
            body=job_body
        ).execute()
    except HttpError as err:
        app.logger.error('BigQuery err: {}'.format(err))
        return insert_query_job(service, query_string, udf, settings)

    app.logger.info(
        'BigQuery Insert Job Response: {}'.format(job['response'])
    )

    return job


def cancel_job(service, job_id):
    try:
        response = service.jobs().cancel(
            projectId=app.config['PROJECT_ID'],
            jobId=job_id
        ).execute()
    except HttpError as err:
        app.logger.error('BigQuery err: {}'.format(err))
        return cancel_job(service, job_id)

    app.logger.info(
        'BigQuery Cancel Job Response: {}'.format(response)
    )
    return response


def get_job(service, job_id):
    try:
        response = service.jobs().get(
            projectId=app.config['PROJECT_ID'],
            jobId=job_id
        ).execute()
    except HttpError as err:
        app.logger.error('BigQuery err: {}'.format(err))
        return get_job(service, job_id)

    app.logger.info(
        'BigQuery Get Job Response: {}'.format(response)
    )

    return response


def get_schema(service, table):
    try:
        response = service.tables().get(
            tableId=table['tableId'],
            datasetId=table['datasetId'],
            projectId=table['projectId']
        ).execute()
    except HttpError as err:
        app.logger.error('BigQuery err: {}'.format(err))
        return get_schema(service, table)

    app.logger.info(
        'BigQuery Get Schema Response: {}'.format(response['schema'])
    )

    return response['schema']


def iterate_elements(service, table, max_result, process, params):
    schema = get_schema(service, table)

    def iterator(page_token):
        try:
            response = service.tabledata().list(
                pageToken=page_token,
                tableId=table['tableId'],
                datasetId=table['datasetId'],
                projectId=table['projectId'],
                maxResults=max_result
            ).execute()
        except HttpError as err:
            app.logger.error('BigQuery err: {}'.format(err))
            return iterator(page_token)
        page_token = response.get('pageToken', None)
        process(
            schema.get('fields', []),
            response.get('rows', []),
            params
        )
        if page_token is not None:
            iterator(page_token)

    iterator(None)


def replace_all(inp, params):
    ret = inp
    for key, value in params.iteritems():
        ret = ret.replace('{{' + key + '}}', unicode(value))
    return ret


def compile_template(template, params):
    if template is None:
        return None
    if params is None:
        return template
    return replace_all(unicode(template), params)


def query(query_string, udf, process, settings, params):

    query_string = compile_template(query_string, params)

    service = get_service()
    service_settings = settings.get('service', {})

    job = insert_query_job(service, query_string, udf, service_settings)

    while job['response']['status']['state'] != 'DONE':
        app.logger.info(
            'Waiting for {} job to complete: {}'.format(
                job['response']['jobReference'],
                job['response']['status']['state']
            )
        )
        if time.time() - job['start_time'] > settings['timeout']:
            app.logger.info(
                'Job timeout: {}'.format(job['response']['jobReference'])
            )
            cancel_job(service, job['job_id'])
            job = insert_query_job(service, query_string, udf, service_settings)

        time.sleep(1.0)
        job['response'] = get_job(service, job['job_id'])

    if 'errorResult' in job['response']['status']:
        app.logger.error(
            'BigQuery Error: {}'.format(
                job['response']['status']['errorResult']
            )
        )
        raise Exception(job['response']['status']['errorResult'])

    dest_table = job['response']['configuration']['query']['destinationTable']

    iterate_elements(
        service,
        dest_table,
        settings['max_result'],
        process,
        params
    )


def add_copy_job(service, route, settings):
    job_id = str(uuid.uuid4())
    job_body = {
        'jobReference': {
            'jobId': job_id
        },
        'configuration': {
            'copy': {
                'sourceTable': {
                    'projectId': app.config['PROJECT_ID'],
                    'datasetId': route['what_dataset'],
                    'tableId': route['what'],
                },
                'destinationTable': {
                    'projectId': app.config['PROJECT_ID'],
                    'datasetId': route['where_dataset'],
                    'tableId': route['where'],
                },
                'createDisposition': 'CREATE_IF_NEEDED',
                'writeDisposition': 'WRITE_APPEND',
                'maximumBillingTier': settings.get('maximumBillingTier', 1)
            }
        }
    }
    try:
        response = service.jobs().insert(
            projectId=app.config['PROJECT_ID'],
            body=job_body
        ).execute()
    except HttpError as err:
        app.logger.error('BigQuery err: {}'.format(err))
        return add_copy_job(service, route, settings)

    app.logger.info(
        'BigQuery Add Copy Job Response: {}'.format(response)
    )

    if 'errorResult' in response['status']:
        if response['status']['errorResult']['reason'] == 'notFound':
            app.logger.warn(
                'BigQuery table does not exits: {}'.format(
                    route['what']
                )
            )
            return None
        else:
            app.logger.error(
                'BigQuery Error: {}'.format(
                    response['status']['errorResult']
                )
            )
            raise Exception(response['status']['errorResult'])

    return response


def copy(what, where, what_dataset, where_dataset, settings):
    service = get_service()
    service_settings = settings.get('service', {})
    return add_copy_job(service, {
        'what': what,
        'where': where,
        'what_dataset': what_dataset,
        'where_dataset': where_dataset
    }, service_settings)
