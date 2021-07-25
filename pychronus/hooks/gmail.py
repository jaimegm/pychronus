from __future__ import print_function

import json
import logging
import os.path
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.hooks.base import BaseHook
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

token_path = "/root/creds/gmail_token.json"
credentials_path = "/root/creds/gmail.json"


class Gmail(BaseHook):
    def __init__(self, userId="me"):
        self.scopes = ["https://mail.google.com/"]
        self._service = None
        self.userId = userId

    @property
    def service(self):
        if self._service is None:
            self._service = build("gmail", "v1", credentials=self.get_creds())
        return self._service

    @staticmethod
    def read_json(filepath):
        with open(filepath, "r") as file:
            json_object = json.load(file)
            file.close()
        return json_object

    @staticmethod
    def write_json(filepath, json_object):
        with open(filepath, "w") as file:
            json.dump(json_object, file)
            file.close()
        return json_object

    def get_token(self):

        server_json = self.read_json(credentials_path)["installed"]
        token_json = self.read_json(token_path)
        params = {
            "grant_type": "refresh_token",
            "client_id": server_json["client_id"],
            "client_secret": server_json["client_secret"],
            "refresh_token": token_json["refresh_token"],
        }
        authorization_url = "https://www.googleapis.com/oauth2/v4/token"

        resp = requests.post(authorization_url, data=params).json()
        logging.info(resp)
        token_json["token"] = resp["access_token"]
        expires_at = datetime.utcnow() + timedelta(seconds=resp["expires_in"])
        token_json["expiry"] = str(datetime.strftime(expires_at, "%Y-%m-%dT%H:%M:%S"))
        return self.write_json(token_path, token_json)

    def get_creds(self):
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, self.scopes)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", self.scopes
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(token_path, "w") as token:
                token.write(creds.to_json())
        return creds

    def search(self, query):
        return (
            self.service.users()
            .messages()
            .list(userId=self.userId, q=query, maxResults=500)
            .execute()
        )

    def batchDelete(self, email_ids):
        return (
            self.service.users()
            .messages()
            .batchDelete(userId=self.userId, body={"ids": email_ids})
            .execute()
        )

    def get_email(self, email_id):
        return (
            self.service.users()
            .messages()
            .get(userId=self.userId, id=email_id)
            .execute()
        )

    def get_binance_deposits(self):
        emails = self.search(query="from:no-reply@revolut.com binance")
        goods = []
        for msg in emails.get("messages"):
            email = self.get_email(email_id=msg.get("id"))
            for x in email.get("payload").get("headers"):
                if x.get("name") == "Subject":
                    subject = x.get("value")
                    deposited = pd.to_datetime(email.get("internalDate"), unit="ms")
                    data = {
                        "email_id": msg.get("id"),
                        "subject": subject,
                        "exchange": subject.split("€")[-1].split(" ")[-1],
                        "deposit": int(
                            subject.split("€")[-1].split(" ")[0].replace(",", str())
                        ),
                        "deposited_on": deposited,
                        "deposited_month": f"{deposited.year}-{deposited.month}",
                    }
                    goods.append(data)
            time.sleep(1)
        return pd.DataFrame(goods)

    def clean_spam(self):
        spammed_emails = [
            "in:spam drive-shares-noreply@google.com",
            "in:spam no-reply@sharepointonline.com",
            "in:spam googlegroups",
            "in:spam whatsapp",
            "in:spam female",
            "in:spam online",
            "in:spam meet",
            "in:spam girl",
            "in:spam looking",
        ]
        spam_ids = []
        for spam_query in spammed_emails:
            spam = self.search(spam_query)
            if spam.get("resultSizeEstimate") != 0:
                spam_ids = spam_ids + [
                    email.get("id") for email in spam.get("messages")
                ]
        if len(spam_ids) != 0:
            self.batchDelete(email_ids=spam_ids)
            logging.info("Cleaned Spam Folder")
        else:
            logging.info("All good in the hood!")
