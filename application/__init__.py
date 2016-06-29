"""
Initialize Flask app

"""
import os
import time
from flask import Flask
from flask_debugtoolbar import DebugToolbarExtension
from werkzeug.debug import DebuggedApplication

from application.urls import add_rules

app = Flask('application')

if ('SERVER_SOFTWARE' in os.environ and
        os.environ['SERVER_SOFTWARE'].startswith('Dev')):
    # Development settings
    app.config.from_object('config.development.Development')
    # Flask-DebugToolbar
    toolbar = DebugToolbarExtension(app)
    app.wsgi_app = DebuggedApplication(app.wsgi_app, evalex=True)
else:
    app.config.from_object('config.production.Production')

# Enable jinja2 loop controls extension
app.jinja_env.add_extension('jinja2.ext.loopcontrols')

# Pull in URL dispatch routes
add_rules(app)
