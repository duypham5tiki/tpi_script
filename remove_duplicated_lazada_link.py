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


def get_item_id_from_link(competitor_link):
    if not competitor_link:
        return None

    search = re.search("-i([\d]+)-s([\d]+)", competitor_link)
    if search is not None:
        return search.group(1)

    search = re.search("/i([\d]+)-s([\d]+)", competitor_link)
    if search is not None:
        return search.group(1)

    search = re.search("-i([\d]+)", competitor_link)
    if search is not None:
        return search.group(1)

    search = re.search("/i([\d]+)", competitor_link)
    if search is not None:
        return search.group(1)
    return None

result = db.session.execute(
    "select id, link from competitor_product_link where site_id=3"
).fetchall()
lazada_id_to_link = defaultdict(list)
for row in result:
    item_id = get_item_id_from_link(row[1])
    if not item_id:
        continue
    lazada_id_to_link[item_id].append(row[0])

duplicated_lazada_id_to_link = defaultdict()
for key, link_id in lazada_id_to_link.items():
    if len(link_id) > 1:
        duplicated_lazada_id_to_link[key] = link_id

len(duplicated_lazada_id_to_link)

for key, link_ids in duplicated_lazada_id_to_link.items():
    print(key)
    print(link_ids)
    main_competitor_product_link_id = link_ids[0]
    competitor_product_link = (
        db.session.query(CompetitorProductLink)
        .filter(CompetitorProductLink.id == main_competitor_product_link_id)
        .first()
    )
    cplm_dict = {}
    competitor_product_link_matches = (
        db.session.query(CompetitorProductLinkMatch)
        .filter(
            CompetitorProductLinkMatch.competitor_product_link_id
            == main_competitor_product_link_id
        )
        .all()
    )
    for competitor_product_link_match in competitor_product_link_matches:
        cplm_dict[
            competitor_product_link_match.tiki_product_id
        ] = competitor_product_link_match
    variances = (
        db.session.query(CompetitorProductVariance)
        .options(joinedload(CompetitorProductVariance.products))
        .filter(
            CompetitorProductVariance.competitor_product_link_id
            == main_competitor_product_link_id
        )
        .all()
    )
    other_links = (
        db.session.query(CompetitorProductLink)
        .filter(CompetitorProductLink.id.in_(link_ids[1:]))
        .filter(CompetitorProductLink.site_id == 3)
        .all()
    )
    if not other_links:
        continue
    other_competitor_product_link_matches: List[CompetitorProductLinkMatch] = (
        db.session.query(CompetitorProductLinkMatch)
        .filter(
            CompetitorProductLinkMatch.competitor_product_link_id.in_(
                list(map(lambda link: link.id, other_links))
            )
        )
        .all()
    )
    for competitor_product_link_match in other_competitor_product_link_matches:
        verification_status = competitor_product_link_match.verification_status
        tiki_product_id = competitor_product_link_match.tiki_product_id
        if (
            verification_status == VerificationStatus.APPROVED
            and tiki_product_id not in cplm_dict.keys()
        ):
            competitor_product_link_match.competitor_product_link_id = (
                main_competitor_product_link_id
            )
            db.session.merge(competitor_product_link_match)
            db.session.commit()
            cplm_dict[tiki_product_id] = competitor_product_link_match
        elif (
            verification_status == VerificationStatus.APPROVED
            and tiki_product_id in cplm_dict.keys()
        ):
            cplm = cplm_dict.get(competitor_product_link_match.tiki_product_id)
            cplm.verified_by = competitor_product_link_match.verified_by
            cplm.verified_on = competitor_product_link_match.verified_on
            cplm.last_added_by = competitor_product_link_match.last_added_by
            cplm.verification_status = VerificationStatus.APPROVED
            db.session.merge(cplm)
            db.session.delete(competitor_product_link_match)
            db.session.commit()
        elif (
            verification_status != VerificationStatus.APPROVED
            and tiki_product_id in cplm_dict.keys()
        ):
            cplm = cplm_dict.get(competitor_product_link_match.tiki_product_id)
            if cplm.verification_status == VerificationStatus.APPROVED:
                db.session.delete(competitor_product_link_match)
        elif (
            verification_status != VerificationStatus.APPROVED
            and tiki_product_id not in cplm_dict.keys()
        ):
            competitor_product_link_match.competitor_product_link_id = (
                main_competitor_product_link_id
            )
            db.session.merge(competitor_product_link_match)
            db.session.commit()
            cplm_dict[tiki_product_id] = competitor_product_link_match
    variance_dict = {cpv.competitor_variance_id: cpv for cpv in variances}
    other_variance_dict = defaultdict(list)
    other_variance_ids = []
    for other_link in other_links:
        competitor_product_variances = (
            db.session.query(CompetitorProductVariance)
            .options(joinedload(CompetitorProductVariance.products))
            .filter(
                CompetitorProductVariance.competitor_product_link_id == other_link.id
            )
            .all()
        )
        for other_variance in competitor_product_variances:
            other_variance_ids.append(other_variance.id)
            if other_variance.competitor_variance_id not in variance_dict.keys():
                other_variance.competitor_product_link_id = (
                    main_competitor_product_link_id
                )
                db.session.merge(other_variance)
                db.session.commit()
                variance_dict[other_variance.competitor_variance_id] = other_variance
            elif other_variance.competitor_variance_id:
                other_variance_dict[other_variance.competitor_variance_id].append(
                    other_variance
                )
    for variant_id, variant in variance_dict.items():
        print(variant_id)
        other_variances = other_variance_dict[variant_id]
        cpvm_dict = {variant.tiki_product_id: variant for variant in variant.products}
        other_competitor_product_variant_matches: List[
            CompetitorProductVarianceMatch
        ] = []
        for other_variant in other_variances:
            for match in other_variant.products:
                other_competitor_product_variant_matches.append(match)
        for (
            competitor_product_variance_match
        ) in other_competitor_product_variant_matches:
            verification_status = competitor_product_variance_match.verification_status
            tiki_product_id = competitor_product_variance_match.tiki_product_id
            if (
                verification_status == VerificationStatus.APPROVED
                and tiki_product_id not in cpvm_dict.keys()
            ):
                competitor_product_variance_match.competitor_product_variance_id = (
                    variant.id
                )
                db.session.merge(competitor_product_variance_match)
                db.session.commit()
                cpvm_dict[tiki_product_id] = competitor_product_variance_match
            elif (
                verification_status == VerificationStatus.APPROVED
                and tiki_product_id in cpvm_dict.keys()
            ):
                cpvm = cpvm_dict.get(competitor_product_variance_match.tiki_product_id)
                if cpvm.verified_on is None or (
                    competitor_product_variance_match.verified_on
                    and competitor_product_variance_match.verified_on > cpvm.verified_on
                ):
                    cpvm.verified_by = competitor_product_variance_match.verified_by
                    cpvm.verified_on = competitor_product_variance_match.verified_on
                cpvm.verification_status = VerificationStatus.APPROVED
                db.session.merge(cpvm)
                db.session.commit()
            elif (
                verification_status != VerificationStatus.APPROVED
                and tiki_product_id in cpvm_dict.keys()
            ):
                cpvm = cpvm_dict.get(competitor_product_variance_match.tiki_product_id)
            elif (
                verification_status != VerificationStatus.APPROVED
                and tiki_product_id not in cpvm_dict.keys()
            ):
                competitor_product_variance_match.competitor_product_variance_id = (
                    variant.id
                )
                db.session.merge(competitor_product_variance_match)
                db.session.commit()
                cpvm_dict[tiki_product_id] = competitor_product_variance_match
    competitor_product_link.link = other_links[-1].link
    db.session.merge(competitor_product_link)
    db.session.commit()
    cpl_ids_condition_string = ",".join(
        list(map(lambda link: str(link.id), other_links))
    )
    variant_ids_condition_string = ",".join(
        list(map(lambda var: str(var), other_variance_ids))
    )
    if variant_ids_condition_string:
        db.session.execute(
            """
        delete from competitor_product_variance_match where competitor_product_variance_id in ({})
        """.format(
                variant_ids_condition_string
            )
        )
        db.session.commit()
    db.session.execute(
        """
    delete from competitor_product_variance where competitor_product_link_id in ({})""".format(
            cpl_ids_condition_string
        )
    )
    db.session.commit()
    db.session.execute(
        """delete from competitor_product_link_match where competitor_product_link_id in ({})""".format(
            cpl_ids_condition_string
        )
    )
    db.session.commit()
    db.session.execute(
        """delete from competitor_product_link where id in ({})""".format(
            cpl_ids_condition_string
        )
    )
    db.session.commit()
    db.session.remove()
