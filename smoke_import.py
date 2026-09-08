"""CI smoke import. Not copied into the Worker image."""
from celery_app import app

assert app.conf.task_track_started
assert app.conf.broker_transport_options["messages_collection"] == "celery.messages"
