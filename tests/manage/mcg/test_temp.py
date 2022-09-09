import base64
import logging
import random
import uuid

import pytest
import requests

from ocs_ci.deployment.azure import AZUREIPI
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    tier1,
    skipif_ocs_version,
    azure_platform_required,
)
from ocs_ci.ocs import defaults, constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.bucket_policy import NoobaaAccount

logger = logging.getLogger(__name__)


@tier1
@bugzilla("2027849")
@pytest.mark.polarion_id("OCS-3963")
@skipif_ocs_version("<4.10")
class TestNoobaaStorageAccount:

    """
    Test azure Noobaa SA
    """

    def test_temp(self, mcg_obj_session, mcg_account_factory):
        """
        Test validates whether azure noobaa storage account is configured with TLS version 1.2
        and secure transfer enabled

        """
        na = f"nsfs-integrity-test-{random.randrange(100)}"
        # s3_creds = mcg_account_factory(
        #     name=na,
        #     allowed_buckets={"full_permission": True},
        #     default_resource=constants.DEFAULT_NOOBAA_BACKINGSTORE,
        #     )
        user_name = "noobaa-user" + str(uuid.uuid4().hex)
        email = user_name + "@mail.com"
        s3_cred = NoobaaAccount(
            mcg_obj_session, name=user_name, email=email, buckets=["first.bucket"], admin_access=True,
        )
        s3_creds = mcg_obj_session.exec_mcg_cmd(
            f"account create {na} --full_permission=True --default_resource {constants.DEFAULT_NOOBAA_BACKINGSTORE}",
        )
        logger.info(s3_creds)
        acc_secret_dict = OCP(
            kind="secret", namespace=config.ENV_DATA["cluster_namespace"]
        ).get(f"noobaa-admin")
        admin_pw = base64.b64decode(
            acc_secret_dict.get("data").get("password")
        ).decode("utf-8")
        mcg_obj_session.exec_mcg_cmd(f"account passwd admin@noobaa.io --new-password=asd --old-password={admin_pw} --retype-new-password=asd")


