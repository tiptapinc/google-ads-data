#!/usr/bin/env python
# encoding: utf-8
#
# Copyright (c) 2022 MotiveMetrics. All rights reserved.
#
"""
Utilities for accessing the customer, account, and authorization info that
is stored in the AppX mongodb database

This module should be replaced or updated to use the accounts microservice
once it is functional.

Two functions are meant to be public:

- account_name_to_cust_id
- cust_id_to_refresh_token

All other functions are internal to this module
"""
import pymongo


def appx_mongo_db(env: str = "prod") -> pymongo.database.Database:
    host = f"mongodb-{env}.motivemetrics.com"
    port = 27017
    client = pymongo.MongoClient(
        host, port, connectTimeoutMS=2000, serverSelectionTimeoutMS=4000
    )
    db = client["meteor-appx"]
    return db


def cust_id_to_account(custId: str, env: str = "prod") -> dict:
    db = appx_mongo_db(env)

    if db is None:
        return None

    accounts = db.AdWordsAccounts

    # this needs to return the freshest account if there are multiple
    account = accounts.find_one({"data.customerId.customerId": custId})
    return account


def account_name_to_account(
    acctname: str, custname: str = "", env: str = "prod"
) -> dict:
    db = appx_mongo_db(env)

    if db is None:
        return None

    criteria = {"type": "google"}
    if custname:
        customers = db.CustomerAccounts
        customer = customers.find_one(
            {"name": {"$regex": f"^{custname}$", "$options": "-i"}}
        )
        if customer is None:
            return None

        criteria["_id"] = {"$in": customer["accounts"]}

    accounts = db.AdWordsAccounts
    criteria["name"] = {"$regex": "^{0}$".format(acctname), "$options": "-i"}
    account = accounts.find_one(criteria)
    return account


def account_to_refresh_token(account: dict) -> str:
    if account is None or "data" not in account:
        return None

    return account["data"].get("refresh_token")


def account_name_to_refresh_token(
    acctname: str, custname: str = "", env: str = "prod"
) -> str:
    account = account_name_to_account(acctname, custname, env)
    return account_to_refresh_token(account)


def account_name_to_cust_id(
    acctname: str, custname: str = "", env: str = "prod"
) -> str:
    """
    Get the Google Ads customer ID for an account identified by its name
    and optionally the MotiveMetrics customer name

    :arg acctname:
        The (case insensitive) name of the account. This parameter is
        sufficient if the name is globally unique amongst MotiveMetrics
        accessible accounts. If the name is not unique, the returned
        account will be one of the accounts with that name. Do not include
        the account type (- Google), that is appended by AppX.

    :arg custname:
        The (case insensitive) name of the MotiveMetrics customer that
        owns the account. Include this parameter if you're not sure that
        the account name is globally unique among customer accounts.

    :arg env:
        Environment from which to retrieve the account information.

    :return:
        The Google Ads ``customer.id`` resource for the account.

    Example::
        custId = account_name_to_cust_id("car.com", custname="autoweb")
    """
    account = account_name_to_account(acctname, custname, env)
    if account is None or "data" not in account or "customerId" not in account["data"]:
        return None

    return account["data"]["customerId"].get("customerId")


def cust_id_to_refresh_token(custId: str, env: str = "prod") -> str:
    """
    get the saved refresh token for the account associated with a Google Ads customer ID

    :param str custId: customer ID
    :param str env: (optional) environment of the AppX mongodb database
    :returns: OAuth refresh token needed to access the account
    :rtype: str
    """
    account = cust_id_to_account(custId, env)
    return account_to_refresh_token(account)
