import logging
import sys
import os

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

logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

app_injector: FlaskInjector = tpi_app.injector

indexer = app_injector.injector.get(
    ManualVerificationDashboardIndexer
)


ID: int = int(os.getenv("ID"))

try:
    print("Delete competitor_link_crawl_status_version")
    db.session.execute(
        """
         delete from competitor_link_crawl_status_version where competitor_product_link_id = {}
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_link_crawl_status")
    db.session.execute(
        """
         delete from competitor_link_crawl_status where competitor_product_link_id = {}
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_product_verification_event")
    db.session.execute(
        """
         delete from competitor_product_verification_event where type = 'COMPETITOR_PRODUCT_VARIANCE' and competitor_object_id in (
            select id from competitor_product_variance where competitor_product_link_id = {}
        )
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_product_verification_event")
    db.session.execute(
        """
         delete from competitor_product_verification_event where type = 'COMPETITOR_PRODUCT_LINK' and competitor_object_id = {}
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_product_shipping_info")
    db.session.execute(
        """
         delete from competitor_product_shipping_info where competitor_product_link_id = {}
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_price_event")
    db.session.execute(
        """
         delete from competitor_price_event  where competitor_variance_id in (
            select id from competitor_product_variance where competitor_product_link_id = {}
        )
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_price_event")
    db.session.execute(
        """
         delete from competitor_price_event where competitor_product_link_id = {}
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_product_coupon_info")
    db.session.execute(
        """
         delete from competitor_product_coupon_info  where competitor_product_link_id = {}
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_flash_sale_info")
    db.session.execute(
        """
         delete from competitor_flash_sale_info where type = 'COMPETITOR_PRODUCT_VARIANCE' and entity_id in (
            select id from competitor_product_variance where competitor_product_link_id = {}
        )
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_flash_sale_info")
    db.session.execute(
        """
         delete from competitor_flash_sale_info where type = 'COMPETITOR_PRODUCT_LINK' and entity_id = {}
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_product_link_match_version")
    db.session.execute(
        """
         delete from competitor_product_link_match_version where competitor_product_link_id = {}
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_product_variance_match_version")
    db.session.execute(
        """
         delete from competitor_product_variance_match_version where competitor_product_variance_id in (
            select id from competitor_product_variance where competitor_product_link_id = {}
        )
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_product_variance_match")
    db.session.execute(
        """
            delete from competitor_product_variance_match where competitor_product_variance_id in (
                select id from competitor_product_variance where competitor_product_link_id = {}
            )
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_product_variance_match")
    db.session.execute(
        """
            delete from competitor_product_variance_match where competitor_product_variance_id in (
                select id from competitor_product_variance where competitor_product_link_id = {}
            )
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_product_variance")
    db.session.execute(
        """
            delete from competitor_product_variance where competitor_product_link_id = {}
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_product_link_match")
    db.session.execute(
        """
            delete from competitor_product_link_match where competitor_product_link_id = {}
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

try:
    print("Delete competitor_product_link")
    db.session.execute(
        """
            delete from competitor_product_link where id = {}
        """.format(ID)
    )
    db.session.commit()
except Exception as e:
    print(str(e))

db.session.remove()
