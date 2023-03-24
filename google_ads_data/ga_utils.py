#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2022 MotiveMetrics. All rights reserved.

Utilities for making Google Ads API queries.

To make a Google Ads API query, we need to get a refresh token
and possibly a customerId from the AppX mongodb database.

With that we can derive the authorizing account's customerId
and get a GoogleAdsService instance with which to make a query.
"""

import boto3
import datetime
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import google.ads.googleads.v11.services.services.google_ads_service.client as google_ads_client
from google.api_core import exceptions
from google.api_core.retry import Retry
from google.protobuf.json_format import MessageToDict
import pandas
import pytz
import re
import typing
import yaml

from .account_utils import cust_id_to_refresh_token

GOOGLE_ADS_API_VERSION = "v12"
CUSTOMER_CLIENT_QUERY = """
    SELECT
      customer_client.status,
      customer_client.client_customer,
      customer_client.manager,
      customer_client.id
    FROM customer_client
    WHERE customer_client.status='ENABLED'
"""

client = boto3.client("ssm")
response = client.get_parameter(Name="keys_google_adwords_api_keys.yml")
GA_KEYS = yaml.safe_load(response["Parameter"]["Value"])

CAMPAIGN_FROM_RESOURCE = "campaign"
CAMPAIGN_FIELDS = ['campaign.id']

MAX_RESULT_SIZE = 2000000

CATEGORICAL_COLS = (
    "ad_group.status",
    "campaign.status",
    "customer.id",
    "customer.status",
    "customer.descriptive_name",
    "ad_group_ad.ad.type",
    "ad_group_ad_asset_view.field_type",
    "ad_group_ad_asset_view.performance_label",
    "ad_group_ad.status"
)


def make_base_ga_config_dict(refresh_token: str) -> dict:
    """
    Return a google ads api config dict

        Parameters:
            refresh_token (str): OAuth refresh token needed to access the account

        Returns:
            config_dict (dict): Google Ads API config info
    """
    config_dict = dict(
        refresh_token=refresh_token,
        client_id=GA_KEYS["client_id"],
        client_secret=GA_KEYS["client_secret"],
        developer_token=GA_KEYS["developer_token"],
        use_proto_plus=True,
    )
    return config_dict


def get_login_customer_id(cust_id: str, refresh_token: str) -> str:
    """
    Return login customer id with provided cust_id and refresh_token

        Parameters:
            cust_id (str): Customer ID
            refresh_token (str): OAuth refresh token needed to access the account

        Returns:
            (str) Login customer ID

    """
    config_dict = make_base_ga_config_dict(refresh_token)

    # get customer Ids for all top-level accounts accessible by the
    # refresh_token
    client = GoogleAdsClient.load_from_dict(config_dict)

    customer_service = client.get_service(
        "CustomerService", version=GOOGLE_ADS_API_VERSION
    )
    resource_names = customer_service.list_accessible_customers().resource_names
    direct_ids = [rn.replace("customers/", "") for rn in resource_names]
    # if cust_id is a top-level account for this refresh_token, then assume
    # it is also the login customerId
    if cust_id in direct_ids:
        return cust_id

    # the account we want data for is a sub-account of the authorizing
    # account, so we need to find out which of the top-level accounts is
    # the parent
    google_ads_service = client.get_service(
        "GoogleAdsService", version=GOOGLE_ADS_API_VERSION
    )
    for parent_id in direct_ids:
        try:
            response = google_ads_service.search(
                customer_id=parent_id,
                query=CUSTOMER_CLIENT_QUERY,
                retry=Retry(maximum=8, deadline=15),
            )
        except GoogleAdsException:
            continue

        for result in response.results:
            if result.customer_client.id == int(cust_id):
                return parent_id

    return None


def build_config_dict(cust_id: str) -> dict:
    """
    Return config dict for provided cust_id

        Parameters:
            cust_id (str): Customer ID

        Returns:
            config_dict (dict): Google Ads API config info

    """
    refresh_token = cust_id_to_refresh_token(cust_id)
    if refresh_token is None:
        return None

    config_dict = make_base_ga_config_dict(refresh_token)
    config_dict["login_customer_id"] = get_login_customer_id(cust_id, refresh_token)
    return config_dict


def get_ga_api_service(cust_id: str, service_name: str) -> google_ads_client.GoogleAdsServiceClient:
    """
    Return a service client instance for the specified service_name

        Parameters:
            cust_id (str): Customer ID
            service_name (str): Google Ads Client service name

        Returns:
            service (google_ads_client.GoogleAdsServiceClient): Google Ads Client service

    """
    config_dict = build_config_dict(cust_id)
    if not config_dict:
        return None

    client = GoogleAdsClient.load_from_dict(config_dict)
    service = client.get_service(service_name, version=GOOGLE_ADS_API_VERSION)
    return service


def camel_to_snake(camel_string: str) -> str:
    """
    Convert and return a string from camel case to snake case

        Parameters:
            camel_string (str): String to be converted

        Returns:
            snake_string (str) Converted snake case string

    """
    pattern = re.compile(r"(?<!^)(?=[A-Z])")
    snake_string = pattern.sub("_", camel_string).lower()
    return snake_string


def snake_to_camel(snake_string: str) -> str:
    """
    Convert and return a string from snake case to camel case

        Parameters:
            snake_string (str): String to be converted

        Returns:
            camel_string (str) Converted camel case string

    """

    temp = snake_string.split("_")
    camel_string = temp[0] + "".join(s.capitalize() for s in temp[1:])
    return camel_string


def get_nested_dict_value(key: str, nested_dict: dict) -> typing.Union[dict, str, int, float]:
    """
    Return value from a nested dict by key

        Parameters:
            key (str):
            nested_dict (dict): nested dict

        Returns:
            value (dict | str | int | float): value of the key from nested dict

    """
    keys = key.split(".")
    value = nested_dict
    for k in keys:
        if k not in value:
            return None

        value = value.get(k)

    return value


def account_time(cust_id: str) -> datetime.datetime:
    """
    Returns a timezone-aware datetime that represents the current
    time in the account's timezone

        Parameters:
            cust_id (str): ``customer.id`` for the Google Ads account

        Returns:
            account_time (datetime.datetime): Account's current time

    """
    service = get_ga_api_service(cust_id, "GoogleAdsService")
    query = "SELECT customer.time_zone FROM customer"
    response = service.search(
        customer_id=cust_id, query=query, retry=Retry(maximum=8, deadline=15)
    )
    row = list(response)[0]
    timezone = pytz.timezone(row.customer.time_zone)
    account_time = datetime.datetime.utcnow().astimezone(timezone)
    return account_time


def account_date(custId: str) -> datetime.date:
    """
    Returns the current date for an account, based on the account's timezone

        Parameters:
            cust_id (str): ``customer.id`` for the Google Ads account

        Returns:
            date (datetime.date): Account's current time
    """
    date = account_time(custId).date()
    return date


def make_base_query(
    cust_id: str,
    from_resource: str,
    fields: typing.List[str],
    start: typing.Union[datetime.date, datetime.datetime] = None,
    end: typing.Union[datetime.date, datetime.datetime] = None,
    zero_impressions: bool = False,
) -> str:
    """
    Make a basic Google Ads Query Language (GAQL) query to be used with
    `GoogleAdsService.SearchStream` or `GoogleAdsService.Search`

        Parameters:
            cust_id (str): The Google Ads ``customer.id`` resource for the account.
            from_resource (str): The Google Ads API resource that fields will be selected from.
            For example ``keyword_view``

            fields (typing.List[str]): The Google Ads API resource fields that you want to return data
            for. For example ``['campaign.name', 'metrics.impressions']``

            start (typing.Union[datetime.date, datetime.datetime]): Start date for metrics. Defaults to the current day
            for the specified customer account.

            end (typing.Union[datetime.date, datetime.datetime]): End date for metrics. Defaults to the current day for
            the specified customer account.

            zero_impressions (bool): Whether to include resources with zero impressions. Default is False.

        Returns:
            query (str): A fully formed GAQL query.

    """
    today = account_date(cust_id)
    if start is None:
        start = today

    if isinstance(start, datetime.datetime):
        start = start.date()

    if end is None:
        end = today

    if isinstance(end, datetime.datetime):
        end = end.date()

    query = "SELECT "
    query += ", ".join(fields)
    query += f" FROM {from_resource} "

    wheres = []
    if zero_impressions is False:
        wheres.append("metrics.impressions > 0")

    start_str = start.strftime("%Y-%m-%d")
    wheres.append(f"segments.date >= '{start_str}'")

    end_str = end.strftime("%Y-%m-%d")
    wheres.append(f"segments.date <= '{end_str}'")

    whereclause = " AND ".join(wheres)
    query += f" WHERE {whereclause}"

    return query


def convert_to_category_dtype(df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Convert dataframe columns to category dtype to save memory

        Parameters:
            df (pandas.DataFrame): dataframe to be converted

        Returns:
            df (pandas.DataFrame): converted dataframe

    """
    for col in CATEGORICAL_COLS:
        if col in df.columns:
            df[col] = df[col].astype('category')

    return df


def execute_query(
    cust_id: str, query: str, fields: typing.List[str]
) -> pandas.DataFrame:
    """
    Execute a GAQL query using ``GoogleAdsService.SearchStream``
    and return the results in a pandas DataFrame

        Parameters:
            cust_id (str): The Google Ads ``customer.id`` resource for the account.
            query (str): A fully-formed GAQL query.
            fields (typing.List[str]): The Google Ads API resource fields that are selected in the query

        Returns:
            A pandas DataFrame with data for each of the requested fields.

    """
    camel_fields = [snake_to_camel(f) for f in fields]

    service = get_ga_api_service(cust_id, "GoogleAdsService")
    stream = service.search_stream(
        customer_id=cust_id,
        query=query,
        retry=Retry(maximum=20, deadline=60)
    )

    rows = []
    try:
        for batch in stream:
            for result in batch.results:
                result_dict = MessageToDict(result._pb)
                row = []
                for field in camel_fields:
                    row.append(get_nested_dict_value(field, result_dict))

                rows.append(row)
    except exceptions.Unknown:
        response = service.search(
            customer_id=cust_id,
            query=query,
            retry=Retry(maximum=20, deadline=60)
        )
        for result in response:
            result_dict = MessageToDict(result._pb)
            row = []
            for field in camel_fields:
                row.append(get_nested_dict_value(field, result_dict))

            rows.append(row)

    df = pandas.DataFrame(rows, columns=fields)
    return convert_to_category_dtype(df)


def check_result_size(cust_id: str, query: str) -> int:
    """
    Make a request with page_size=1 and return_total_results_count=True
    to get number of results we'll get from this query

        Parameters:
            cust_id (str): The Google Ads ``customer.id`` resource for the account.
            query (str): A fully-formed GAQL query.

        Returns:
            count (int): An int indicates number of results we'll get from this query.

    """
    config_dict = build_config_dict(cust_id)
    client = GoogleAdsClient.load_from_dict(config_dict)
    search_request = client.get_type("SearchGoogleAdsRequest")

    query_split = query.split('FROM')

    size_query = f"{query_split[0].split(', ')[0]} FROM {query_split[1]}"

    search_request.customer_id = cust_id
    search_request.query = size_query
    search_request.page_size = 1
    search_request.return_total_results_count = True

    service = client.get_service("GoogleAdsService", version=GOOGLE_ADS_API_VERSION)
    results = service.search(request=search_request)

    count = results.total_results_count
    return count


def get_ga_data(
    cust_id: str,
    from_resource: str,
    fields: typing.List[str],
    start: typing.Union[datetime.date, datetime.datetime] = None,
    end: typing.Union[datetime.date, datetime.datetime] = None,
    zero_impressions: bool = False,
    wheres: typing.List[str] = [],
) -> pandas.DataFrame:
    """
    Get a pandas dataframe of Google Ads data for a customer account

        Parameters:
            cust_id (str): The Google Ads ``customer.id`` resource for the account.
            from_resource (str): The Google Ads API resource that fields will be selected from.
            For example ``keyword_view``.

            fields (typing.List[str]): The Google Ads API resource fields that are selected in the query.

            start (typing.Union[datetime.date, datetime.datetime]): Start date for metrics. Defaults to the current day
            for the specified customer account.

            end (typing.Union[datetime.date, datetime.datetime]): End date for metrics. Defaults to the current day for
            the specified customer account.

            zero_impressions (bool): Whether to include resources with zero impressions. Default is False.

            wheres (typing.List[str]): Additional 'WHERE' clauses to add to the GAQL query. For example
            ``ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD'``. Default is [].

        Returns:
            df (pandas.DataFrame): A pandas DataFrame with data for each of the requested fields.

    """
    query = make_base_query(cust_id, from_resource, fields, start, end, zero_impressions)

    if wheres:
        query += " AND " + " AND ".join(wheres)

    df = execute_query(cust_id, query, fields)

    return df
