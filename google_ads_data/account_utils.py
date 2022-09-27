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
    """
    Create and return a mongo meteor-appx collection connection

        Parameters:
            env (str): Environment from which to retrieve the account information. Default "prod".

        Returns:
            db (pymongo.database.Database): Mongo Database connection.

    """
    host = f"mongodb-{env}.motivemetrics.com"
    port = 27017
    client = pymongo.MongoClient(
        host, port, connectTimeoutMS=2000, serverSelectionTimeoutMS=4000
    )
    db = client["meteor-appx"]
    return db


def cust_id_to_account(cust_id: str, env: str = "prod") -> dict:
    """
    Return an account document by using cust_id to find a match

        Parameters:
            cust_id (str): customer ID
            env (str): Environment from which to retrieve the account information. Default "prod".

        Returns:
            account (dict): AdWordsAccounts document

    """
    db = appx_mongo_db(env)

    if db is None:
        return None

    accounts = db.AdWordsAccounts

    # this needs to return the freshest account if there are multiple
    account = accounts.find_one({"data.customerId.customerId": cust_id})
    return account


def account_name_to_account(
    account_name: str, cust_name: str = "", env: str = "prod"
) -> dict:
    """
    Return an account document by using account_name and cust_name to find a match

        Parameters:
            account_name (str): The (case insensitive) name of the account. This parameter is
            sufficient if the name is globally unique amongst MotiveMetrics
            accessible accounts. If the name is not unique, the returned
            account will be one of the accounts with that name. Do not include
            the account type (- Google), that is appended by AppX.

            cust_name (str): The (case insensitive) name of the MotiveMetrics customer that
            owns the account. Include this parameter if you're not sure that
            the account name is globally unique among customer accounts.

            env (str): Environment from which to retrieve the account information. Default "prod".

        Returns:
            account (dict): AdWordsAccounts document

    """
    db = appx_mongo_db(env)

    if db is None:
        return None

    criteria = {"type": "google"}
    if cust_name:
        customers = db.CustomerAccounts
        customer = customers.find_one(
            {"name": {"$regex": f"^{cust_name}$", "$options": "-i"}}
        )
        if customer is None:
            return None

        criteria["_id"] = {"$in": customer["accounts"]}

    accounts = db.AdWordsAccounts
    criteria["name"] = {"$regex": "^{0}$".format(account_name), "$options": "-i"}
    account = accounts.find_one(criteria)
    return account


def account_to_refresh_token(account: dict) -> str:
    """
    Return a refresh_token for the account

        Parameters:
            account (dict): AdWordsAccounts document

        Returns:
            refresh_token (str): OAuth refresh token needed to access the account

    """
    if account is None or "data" not in account:
        return None

    refresh_token = account["data"].get("refresh_token")
    return refresh_token


def account_name_to_refresh_token(
    account_name: str, cust_name: str = "", env: str = "prod"
) -> str:
    """
    Return a refresh_token for the account_name/cust_name combo

        Parameters:
            account_name (str): The (case insensitive) name of the account. This parameter is
            sufficient if the name is globally unique amongst MotiveMetrics
            accessible accounts. If the name is not unique, the returned
            account will be one of the accounts with that name. Do not include
            the account type (- Google), that is appended by AppX.

            cust_name (str): The (case insensitive) name of the MotiveMetrics customer that
            owns the account. Include this parameter if you're not sure that
            the account name is globally unique among customer accounts.

            env (str): Environment from which to retrieve the account information. Default "prod".

        Returns:
            refresh_token (str): OAuth refresh token needed to access the account

    """
    account = account_name_to_account(account_name, cust_name, env)
    refresh_token = account_to_refresh_token(account)
    return refresh_token


def account_name_to_cust_id(
    account_name: str, cust_name: str = "", env: str = "prod"
) -> str:
    """
    Get the Google Ads customer ID for an account identified by its name
    and optionally the MotiveMetrics customer name
        
        Parameters:
            account_name (str): The (case insensitive) name of the account. This parameter is
            sufficient if the name is globally unique amongst MotiveMetrics
            accessible accounts. If the name is not unique, the returned
            account will be one of the accounts with that name. Do not include
            the account type (- Google), that is appended by AppX.

            cust_name (str): The (case insensitive) name of the MotiveMetrics customer that
            owns the account. Include this parameter if you're not sure that
            the account name is globally unique among customer accounts.

            env (str): Environment from which to retrieve the account information. Default "prod".

        Returns:
            cust_id (str): The Google Ads ``customer.id`` resource for the account.

    """
    account = account_name_to_account(account_name, cust_name, env)
    if account is None or "data" not in account or "customerId" not in account["data"]:
        return None

    cust_id = account["data"]["customerId"].get("customerId")
    return cust_id


def cust_id_to_refresh_token(cust_id: str, env: str = "prod") -> str:
    """
    Get the saved refresh token for the account associated with a Google Ads customer ID
    
        Parameters:
            cust_id (str): customer ID
            env (str): Environment from which to retrieve the account information. Default "prod".

        Returns:
            refresh_token (str): OAuth refresh token needed to access the account

    """
    account = cust_id_to_account(cust_id, env)
    refresh_token = account_to_refresh_token(account)
    return refresh_token
