***************
google-ads-data
***************

.. contents:: Table of Contents

Introduction
============

``google-ads-data`` is a MotiveMetrics-specific package to get Google Ads data
for MotiveMetrics customers, using the Google Ads API.

This package is under construction. All functions that are intended to be public
are documented, and even those are subject to change. Any functions not included
in this documentation are subject to change or disappearance.

Overview
========

The primary functon of the google-ads-data package is to return ``pandas``
dataframes containing reports data from the Google Ads API.

The Google Ads API reports reference pages can be found
`here <https://developers.google.com/google-ads/api/fields/v11/overview/>`_.

For example, the following will get the most recent day's keywords data for
the `car.com` account of the customer `autoweb`.

.. code:: python

    from google_ads_data import account_name_to_cust_id, get_ga_data

    custId = account_name_to_cust_id("car.com", custname="autoweb")
    fields = [
        "campaign.id",
        "campaign.name",
        "campaign.status",
        "ad_group.id",
        "ad_group.name",
        "ad_group.status",
        "ad_group_criterion.criterion_id",
        "ad_group_criterion.keyword.text",
        "ad_group_criterion.keyword.match_type",
        "ad_group_criterion.status",
        "metrics.impressions",
        "metrics.clicks",
        "metrics.conversions",
        "segments.date",
        "customer.id",
        "customer.status",
        "customer.descriptive_name",
    ]
    fromResource = "keyword_view"
    wheres = ["campaign.name = 'Chevrolet.Search.OEM'"]

    df = get_ga_data(custId, fromResource, fields, wheres=wheres)

Get It Now
==========

::

    $ pip install -e git+https://github.com/motivemetrics/google-ads-data.git#egg=google_ads_data

Documentation
=============

Full documentation is available at https://motivemetrics.github.io/google-ads-data/html/index.html
