from __future__ import print_function

import os.path
import pickle
import string

import pandas as pd
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


class GSheetHook(BaseHook):
    """
    Acceess Googles Spreadsheet API through this hook.
    :param google_conn_id: Connection Id containing the OAuth credentials.
    User 'login' to store client_id, 'password' for client_secret
    and {"refresh_token": "your_refresh_token"} in extra field to provide the refresh token.
    To obtain a refresh token with an existing client_id and client_secrect
    :type google_conn_id: str
    """

    def __init__(
        self, google_conn_id="google_sheets_default"
    ):  # pylint: disable=super-init-not-called
        self.google_conn_id = google_conn_id
        self._credentials = None

    def get_conn(self):
        return self.get_connection(self.google_conn_id)

    def get_service(self):
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        credential_path = Variable.get("CREDENTIALS_PATH")
        pickle_path = os.path.join(credential_path, "token.pickle")
        if os.path.exists(pickle_path):
            with open(pickle_path, "rb") as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                key_path = os.path.join(credential_path, "credentials.json")
                flow = InstalledAppFlow.from_client_secrets_file(
                    key_path, "https://www.googleapis.com/auth/spreadsheets"
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(pickle_path, "wb") as token:
                pickle.dump(creds, token)
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return service

    def get_spreadsheet(self, spreadsheet_id):
        return self.get_service().get(
            spreadsheetId=spreadsheet_id
        )  # pylint: disable=no-member

    def get_values(self, spreadsheet_id, range_, dimension="ROWS"):
        """
        Retrieve data from google spread sheet as a list of list.

        :param spreadsheet_id: id of spreadsheet, find the id in the url of a spreadsheet.
        :type spreadsheet_id: str

        :param range_: Range for obtaining data. Syntax is like
        '<sheet_name>!<start_cell>:<end_cell>'.  Example: 'User!A1:K'
        :type range: str

        :param dimension: 'ROWS' or 'COUMNS' Defaults to 'COLUMNS'
        :type dimension: str
        """
        return (
            self.get_service()
            .spreadsheets()
            .values()
            .get(  # pylint: disable=no-member
                spreadsheetId=spreadsheet_id, range=range_, majorDimension=dimension
            )
            .execute()["values"]
        )

    def get_values_df(self, spreadsheet_id, range_, shape_column=None):
        """
        Retrieve data as pandas.DataFrame. It assumes that the
        first row of the data contains the column names.
        Column names will be transformed (lower cased and hyphens are replaced by underscores)

        :param spreadsheet_id: id of spreadsheet, find the id in the url of a spreadsheet.
        Such an url looks like https://docs.google.com/spreadsheets/d/<spreadsheet_id>?other_stuff=1
        :type spreadsheet_id: str

        :param range_: Range for obtaining data. Syntax is like
        '<sheet_name>!<start_cell>:<end_cell>'.  Example: 'User!A1:K'
        :type range: str

        :param shape_column: title of the column that determines the shape of the resulting
        dataframe.
        :type shape_column: Optional[str]
        """
        values = self.get_values(spreadsheet_id, range_, "COLUMNS")
        result = {self.keyify(column[0]): column[1:] for column in values}

        return pd.DataFrame(self.reshape(result, shape_column=shape_column))

    def truncate_values(self, spreadsheet_id, range_, header=True):
        """Truncates Entire datarange, Its recommended to leave out header columns"""
        if header:
            range_ = self.remove_header(range_)

        response = (
            self.get_service()
            .spreadsheets()
            .values()
            .clear(  # pylint: disable=no-member
                spreadsheetId=spreadsheet_id, range=range_
            )
            .execute()
        )
        return response

    def write_values(self, spreadsheet_id, sheetname, dataframe, start_cell="A1"):
        # Note: Json decoder doesnt play well with Decimals or Datetime obejects, cast varchar
        range_ = self.calc_datarange(sheetname, dataframe, start_cell)
        # Format Dataframe to Write to GoogleSheets
        gsheet_docdata = {}
        # Dataframe must be formatted as a list
        body = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
        gsheet_docdata["majorDimension"] = "ROWS"
        gsheet_docdata["values"] = body
        gsheet_docdata["range"] = range_

        # Write data to Google Sheets
        response = (
            self.get_service()
            .spreadsheets()
            .values()
            .update(  # pylint: disable=no-member
                spreadsheetId=spreadsheet_id,
                range=range_,
                valueInputOption="USER_ENTERED",
                body=gsheet_docdata,
            )
            .execute()
        )
        return response

    @staticmethod
    def calc_datarange(sheetname, df, start_cell="A1"):
        if start_cell == "A1":
            last_column_letter = GSheetHook.column_letter(len(df.columns)) + str(
                len(df.index) + 1
            )
        else:
            int_col = GSheetHook.column_letter2num(start_cell[0])
            int_row = int(start_cell[1:])
            last_column_letter = GSheetHook.column_letter(
                int_col + len(df.columns) - 1
            ) + str(len(df.index) + int_row + 1)
        range_ = str(sheetname + "!" + start_cell + ":" + last_column_letter)
        return range_

    @staticmethod
    def remove_header(range_):
        sheet_name = str(range_.split("!", 1)[0] + "!")
        end_range = ":" + range_.split(":", 1)[1]
        start_range = str(range_.split(":", 1)[0].split("!", 1)[1])
        start_column_letter = start_range[0]
        start_range = int(start_range[1:]) + 1
        range_ = sheet_name + start_column_letter + str(start_range) + end_range
        return range_

    @staticmethod
    def column_letter(column_number):
        letter = ""
        while column_number > 0:
            tmp = (column_number - 1) % 26
            letter = f"{chr(65 + tmp)}{letter}"
            column_number = (column_number - (tmp)) // 26
        return str(letter)

    @staticmethod
    def column_letter2num(column_letter):
        num = 0
        for column in column_letter:
            if column in string.ascii_letters:
                num = num * 26 + (ord(column.upper()) - ord("A")) + 1
        return int(num)

    @staticmethod
    def keyify(original_key):
        replacements = [
            ("ß", "ss"),
            ("ä", "ae"),
            ("ö", "oe"),
            ("ü", "ue"),
            ("-", "_"),
            (" ", "_"),
            (":", ""),
        ]
        replaced = original_key.lower()
        for umlaut, replacement in replacements:
            replaced = replaced.replace(umlaut, replacement)
        return replaced

    @staticmethod
    def reshape(data, shape_column=None):
        if not shape_column:
            length = min(len(column) for column in data.values())
            return {key: value[:length] for key, value in data.items()}

        shape_series = data.get(shape_column, data.get(GSheetHook.keyify(shape_column)))
        length = len(shape_series)

        result = {}
        for key, series in data.items():
            missing_data = [None] * max((0, (length - len(series))))
            result[key] = series[:length] + missing_data
        return result
