from oauth2client.contrib.appengine import AppAssertionCredentials
import httplib2
import apiclient


def build_request(_, *args, **kwargs):
    credentials = AppAssertionCredentials(
        scope='https://www.googleapis.com/auth/bigquery'
    )
    new_http = credentials.authorize(httplib2.Http())
    return apiclient.http.HttpRequest(
        new_http,
        *args,
        **kwargs
    )
