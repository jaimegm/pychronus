import os.path
import string

import pandas as pd
from airflow.hooks.base import BaseHook
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


class GSheetHook(BaseHook):
    """
    Acceess Googles Spreadsheet API through this hook.
    User 'login' to store client_id, 'password' for client_secret
    and {"refresh_token": "your_refresh_token"} in extra field to provide the refresh token.
    To obtain a refresh token with an existing client_id and client_secrect
    """

    def __init__(self):  # pylint: disable=super-init-not-called
        self.scope = ["https://www.googleapis.com/auth/spreadsheets"]
        self.token_path = "/root/creds/gsheet_token.json"
        self._credentials = None

    def get_creds(self):
        if os.path.exists(self.token_path):
            creds = Credentials.from_authorized_user_file(self.token_path, self.scope)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "/root/creds/credentials.json", self.scope
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_path, "w") as token:
                token.write(creds.to_json())
        return creds

    def get_service(self):
        service = build(
            "sheets", "v4", credentials=self.get_creds(), cache_discovery=False
        )
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
