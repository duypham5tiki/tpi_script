import logging
import os
import re
import sys
from collections import defaultdict
from typing import List

import sentry_sdk
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.flask import FlaskIntegration
from sqlalchemy.orm import joinedload
from werkzeug import run_simple
from werkzeug.middleware.dispatcher import DispatcherMiddleware

from retail_promotion.factory import create_promotion_app
from tpi_app.celery_utils import celery
from tpi_app.extensions import db
from tpi_app.factory import create_app
from tpi_app.models.competitor_product_link import CompetitorProductLink
from tpi_app.models.competitor_product_link_match import CompetitorProductLinkMatch
from tpi_app.models.competitor_product_variance import CompetitorProductVariance
from tpi_app.models.competitor_product_variance_match import (
    CompetitorProductVarianceMatch,
)
from tpi_app.models.competitor_product_verification import VerificationStatus
from tpi_app.settings import SENTRY_DSN, ENVIRONMENT

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


def _remove_www(link):
    return re.sub(r"^(https|http)(://)(www.)", r"\1\2", link)


def get_pdp_link(competitor_link):
    link = "https://lazada.vn/products/"
    search = re.search("-i([\d]+)-s([\d]+)", competitor_link)
    if search is not None:
        i_id = search.group(1)
        return link + "i" + i_id + ".html"

    search = re.search("/i([\d]+)-s([\d]+)", competitor_link)
    if search is not None:
        i_id = search.group(1)
        return link + "i" + i_id + ".html"

    search = re.search("-i([\d]+)", competitor_link)
    if search is not None:
        i_id = search.group(1)
        return link + "i" + i_id + ".html"

    search = re.search("/i([\d]+)", competitor_link)
    if search is not None:
        i_id = search.group(1)
        return link + "i" + i_id + ".html"

    return _remove_www(competitor_link)

FROM = int(os.getenv("FROM"))

result = db.session.execute(
    "select id, link from competitor_product_link where site_id=3"
).fetchall()
lazada_id_to_link = defaultdict(list)

count = 1
for row in result:
    if count < FROM:
        continue
    print("{}/{}", count, len(result))
    link = get_pdp_link(row[1])
    try:
        db.session.execute(
            """update competitor_product_link set link = '{}' where id = {}""".format(
                link, row[0]
            )
        )
        db.session.commit()
        count += 1
    except:
        pass

db.session.remove()
