import win32com.client as win32
import pandas as pd
from datetime import datetime

def update_dashboard(dashboard_path:str):
    """Updates dashboard, adds Numerator and Denominator data to Dashboard Sheet"""
    excel=win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(dashboard_path)
    #Refresh Dashboard with new data
    wb.RefreshAll()
    wb.Save()
    excel.Application.Quit()
    return

def save_to_excel(numerator, denominator, path: str) -> None :
    '''Saves dataframe to Server and autofits columns'''
    #Saves Sheets to Excel
    writer = pd.ExcelWriter(path, engine = 'xlsxwriter')
    denominator.to_excel(writer, header=True, index=False, sheet_name = 'Denominator')
    numerator.to_excel(writer, header=True, index=False, sheet_name = 'Numerator')
    writer.close()  
    #Autofit Column width
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(path)
    denom = wb.Worksheets("Denominator")
    num= wb.Worksheets("Numerator")
    denom.Columns.AutoFit()
    num.Columns.AutoFit()
    wb.Save()
    excel.Application.Quit()
    print("Report file saved to server.")
    return

def combined_df(fetch_fn, start_year: int, end_year: int) -> pd.DataFrame:
    """
    Build a combined DataFrame for all available months and years using a data-fetching function.

    Args:
        fetch_fn (callable): Function that takes (year, month) and returns a pandas DataFrame.
        start_year (int): First year (inclusive).
        end_year (int): Last year (inclusive).

    Returns:
        pd.DataFrame: Combined DataFrame for all valid (year, month) combinations.
    """
    frames = []
    current_year = datetime.today().year
    current_month = datetime.today().month

    for year in range(start_year, end_year + 1):
        if year>current_year:
            break
        for month in range(1, 13):
            print(year, month)
            # Skip future months of the current year
            if year == current_year and month >= current_month:
                break

            # Handle December report (when current_month == January)
            if current_month == 1 and year == current_year:
                for prev_month in range(1, 13):
                    frames.append(fetch_fn(year - 1, prev_month))
                break  # after filling December data, stop for this year

            # Normal case
            frames.append(fetch_fn(year, month))
            print(f"Data extracted for year: {year}, month: {month}")

    return pd.concat(frames, ignore_index=True)