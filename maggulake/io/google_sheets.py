import time
from typing import Optional

import gspread
import pandas as pd
from gspread.exceptions import SpreadsheetNotFound
from oauth2client.service_account import ServiceAccountCredentials


def get_client(
    credentials: ServiceAccountCredentials,
) -> gspread.client.Client:
    """Get the gspread client"""
    return gspread.authorize(credentials)


def get_spreadsheet(
    sheet_url: str,
    credentials: ServiceAccountCredentials,
    client: Optional[gspread.client.Client] = None,
) -> gspread.Spreadsheet:
    """Get the google spreadsheet"""

    # Authenticate the client
    if client is None:
        client = get_client(credentials)

    # Exponential backoff
    spreadsheet = None
    for i in range(1, 11):
        try:
            spreadsheet = client.open_by_url(sheet_url)
            break
        except SpreadsheetNotFound as e:
            print(f"Error opening the spreadsheet: {e}")
            time.sleep(2**i)

    if spreadsheet is None:
        raise ValueError("Failed to open the spreadsheet after multiple attempts.")

    return spreadsheet


def read_google_sheet(
    sheet_url: Optional[str] = None,
    sheet_name: Optional[str] = None,
    credentials: Optional[ServiceAccountCredentials] = None,
    client: Optional[gspread.client.Client] = None,
    spreadsheet: Optional[gspread.Spreadsheet] = None,
) -> pd.DataFrame:
    """Read the google spreadsheet and get the dataframe"""

    # Read the file
    if not isinstance(spreadsheet, gspread.Spreadsheet):
        spreadsheet = get_spreadsheet(sheet_url, credentials, client)
    else:
        # precisa ter passado spreadsheet e sheet_name
        if not sheet_name or not spreadsheet:
            raise ValueError("You must pass both sheet_name and spreadsheet")

    # Select the desired sheet
    worksheet = spreadsheet.worksheet(sheet_name)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    return df
