import logging
import sys

import sentry_sdk
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.flask import FlaskIntegration
from tpi_app.celery_utils import celery
from tpi_app.extensions import db
from tpi_app.factory import create_app
from tpi_app.indexer.manual_verification_indexer import (
    ManualVerificationDashboardIndexer,
)
from tpi_app.settings import SENTRY_DSN, ENVIRONMENT
from tpi_base.flask_injector import FlaskInjector

sentry_sdk.init(
    SENTRY_DSN,
    integrations=[FlaskIntegration(), CeleryIntegration()],
    environment=ENVIRONMENT,
)
if ENVIRONMENT == "UAT":
    from dotenv import load_dotenv

    load_dotenv(".env." + ENVIRONMENT.lower())

# for mac m1 uncomment this code below for run local
# import pymysql
# pymysql.install_as_MySQLdb()

tpi_app = create_app(celery=celery)

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
logging.getLogger("kafka").setLevel(logging.DEBUG)

app_injector: FlaskInjector = tpi_app.injector

indexer = app_injector.injector.get(
    ManualVerificationDashboardIndexer
)

products = db.session.execute(
    "select id from tiki_product_info where master_id is null"
).fetchall()
product_ids = list(map(lambda item: item[0], products))
n = 50
chunks = [product_ids[i:i + n] for i in range(0, len(product_ids), n)]

for id_list in chunks:
    indexer.index_for_master_product_ids(id_list)
