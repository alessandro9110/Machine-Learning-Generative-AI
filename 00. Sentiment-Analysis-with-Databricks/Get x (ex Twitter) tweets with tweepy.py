# Databricks notebook source
# MAGIC %pip install tweepy

# COMMAND ----------

# MAGIC %md ### Tweepy Authentication and Initialization

# COMMAND ----------

import tweepy

# Your app's bearer token can be found under the Authentication Tokens section
# of the Keys and Tokens tab of your app, under the
# Twitter Developer Portal Projects & Apps page at
# https://developer.twitter.com/en/portal/projects-and-apps
bearer_token = "AAAAAAAAAAAAAAAAAAAAAFyFsgEAAAAAncL8bimK8ETZcwKAsMYpHQJgmSg%3DmqAIqi0LQrFj9CxTFRBKQi4Y16SNJ4a5hUpV4aXqob4KXK60Qw"

# https://developer.twitter.com/en/portal/projects-and-apps
consumer_key = "IRu4KmY4mARBL3qCKwqN7Kvkh"
consumer_secret = "HEvDPqpiFyDllR8aIP4BQJNzJlLLQnUYC4PjueZFxWy4M7Po3s"

# Your account's (the app owner's account's) access token and secret for your
# app can be found under the Authentication Tokens section of the
# Keys and Tokens tab of your app, under the
# Twitter Developer Portal Projects & Apps page at
# https://developer.twitter.com/en/portal/projects-and-apps
access_token = "151022548-3xJoHcdFMgA4uXdiUwBsSPw6OoaEcsILR9b8RLhf"
access_token_secret = "b0OEV0p8F90PbAGMOWxs3oiUUCJIE8O38kmNx1jDI8MpG"

client_id="UWJwNnZtZEtLZVJ4N204OThwMWQ6MTpjaQ"
client_secret = "YBvbGFDgXcmJms6ut73R_IICcSx3E390d-KdY8y4qnL8-oeaVE"


# COMMAND ----------

import requests
import json
import time
import random
import os

endpoint_url = "https://api.twitter.com/2/users/by?usernames"

query_parameters = {
    "user.fields": "description",
    "expansions": 'pinned_tweet_id',
}


headers = {"Authorization": "Bearer {}".format(bearer_token)}

def connect_to_endpoint(endpoint_url: str, headers: dict, parameters: dict) -> json:
    """
    Connects to the endpoint and requests data.
    Returns a json with Twitter data if a 200 status code is yielded.
    Programme stops if there is a problem with the request and sleeps
    if there is a temporary problem accessing the endpoint.
    """
    response = requests.request(
        "GET", url=endpoint_url, headers=headers, params=parameters
    )
    response_status_code = response.status_code
    if response_status_code != 200:
        if response_status_code >= 400 and response_status_code < 500:
            raise Exception(
                "Cannot get data, the program will stop!\nHTTP {}: {}".format(
                    response_status_code, response.text
                )
            )
        
        sleep_seconds = random.randint(5, 60)
        print(
            "Cannot get data, your program will sleep for {} seconds...\nHTTP {}: {}".format(
                sleep_seconds, response_status_code, response.text
            )
        )
        time.sleep(sleep_seconds)
        return connect_to_endpoint(endpoint_url, headers, parameters)
    return response.json()


json_response = connect_to_endpoint(endpoint_url, headers, query_parameters)

# COMMAND ----------


