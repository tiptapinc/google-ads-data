import datetime
import pdb
import pytest

from google_ads_data import account_name_to_cust_id, get_ga_data

CUSTNAME = "motivemetrics internal"
ACCTNAME = "mm-adwords"

FIELDS = [
    "campaign.id",
    "campaign.name",
    "campaign.status",
    "metrics.impressions"
]

FROM_RESOURCE = "campaign"


def test_get_ga_data():
    pdb.set_trace()
    cust_id = account_name_to_cust_id(ACCTNAME, CUSTNAME)
    end = datetime.datetime.now()
    start = end - datetime.timedelta(weeks=13)
    df = get_ga_data(cust_id, FROM_RESOURCE, FIELDS, start, end, zeroImpressions=True)
    assert df.empty is False
