import  
import pandas as pd
from datetime import datetime
import calendar
import win32com.client as win32
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from dotenv import load_dotenv
import os
import numpy as np
from prefect import flow, task
from dateutil.relativedelta import relativedelta

@task(log_prints=True)
def extraction(year: int, month: int) -> pd.DataFrame:
    """Extraction and Transformation script from SQL"""
    #Returns Last day of the month
    #Returns Last date of the month in the format YYYY-MM-DD
    last_date=datetime.strptime(f'{year}-{month:02}-{calendar.monthrange(year, month)[1]}', '%Y-%m-%d').date()
    #Reference date
    reference_date=f'{year}-{month:02}-01'
    
    query=f'''WITH Denom AS (SELECT  
    dbo.Animal.AnimalID, 
    dbo.Animal.Name,
    dbo.refSpecies.Species, 
    dbo.txnVisit.IntakeType as IntakeType,
    cast(dbo.AnimalDetails.DateOfBirth as date) as DateOfBirth,
    cast(dbo.txnVisit.tin_DateCreated as Date) AS IntakeDate, 
    dbo.txnVisit.tOut_DateCreated,
    DATEDIFF(week, DateOfBirth, dbo.txnVisit.tin_DateCreated ) as IntakeAge

    FROM  dbo.Animal INNER JOIN
           dbo.refSpecies ON dbo.Animal.SpeciesID = dbo.refSpecies.SpeciesID INNER JOIN
           dbo.AnimalDetails ON dbo.Animal.AnimalID = dbo.AnimalDetails.AnimalID INNER JOIN
           dbo.txnVisit ON dbo.Animal.AnimalID = dbo.txnVisit.AnimalID
    WHERE dbo.txnVisit.tin_DateCreated Between '2017-01-01' AND '{last_date}'
    AND dbo.txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender')
    --Excludes animals that had outcome before first day
    AND dbo.txnVisit.tOut_DateCreated >= '{reference_date}'),
    
    --Weeks Table from inbuilt table in SQL Server for Crossjoin 
    Weeks AS (
    SELECT DATEADD(DAY, number,'{reference_date}') AS week_start
    FROM master..spt_values
    WHERE type = 'P' AND number in(0,7,14,21,27)
    ),
    
    Numerator as (
    SELECT DISTINCT
      dbo.Animal.AnimalID AS animalid,
      dbo.Animal.Name,
      dbo.refSpecies.Species,
      CAST(dbo.AnimalDetails.DateOfBirth AS date) AS DateOfBirth,
      dbo.txnVisit.IntakeType,
      dbo.txnVisit.IntakeSubType,
      CAST(dbo.txnVisit.tin_DateCreated AS date) AS IntakeDate,
      CASE
        WHEN dbo.txnVisit.OutComeType = 'PreEuthanasia' THEN 'Euthanised'
        WHEN dbo.txnVisit.OutComeType = 'Died' THEN 'Died'
      END AS OutcomeType,
      CAST(dbo.txnVisit.tOut_DateCreated AS date) AS OutcomeDate
    FROM dbo.Animal
    INNER JOIN dbo.refSpecies
      ON dbo.Animal.SpeciesID = dbo.refSpecies.SpeciesID
    INNER JOIN dbo.AnimalDetails
      ON dbo.Animal.AnimalID = dbo.AnimalDetails.AnimalID
    INNER JOIN dbo.txnVisit
      ON dbo.Animal.AnimalID = dbo.txnVisit.AnimalID
    WHERE  (dbo.txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender'))
    AND dbo.AnimalDetails.DateOfBirth IS NOT NULL
    AND (dbo.txnVisit.OutComeType IN ('Died', 'PreEuthanasia'))
    AND dbo.txnVisit.tin_DateCreated Between '2017-01-01' AND '{last_date}'
    
    UNION
    
    SELECT DISTINCT
      dbo.Animal.AnimalID,
      dbo.Animal.Name,
      dbo.refSpecies.Species,
      CAST(dbo.AnimalDetails.DateOfBirth AS date) AS DateOfBirth,
      dbo.txnVisit.IntakeType,
      dbo.txnVisit.IntakeSubType,
      CAST(dbo.txnVisit.tin_DateCreated AS date) AS IntakeDate,
      'Euthanised' AS OutcomeType,
      CAST(dbo.Euthanasia.DateCreated AS date) AS EuthDate
    FROM dbo.Animal
    INNER JOIN dbo.AnimalDetails
      ON dbo.Animal.AnimalID = dbo.AnimalDetails.AnimalID
    INNER JOIN dbo.refSpecies
      ON dbo.Animal.SpeciesID = dbo.refSpecies.SpeciesID
    INNER JOIN dbo.txnVisit
      ON dbo.Animal.AnimalID = dbo.txnVisit.AnimalID
    INNER JOIN dbo.Euthanasia
      ON dbo.Animal.AnimalID = dbo.Euthanasia.AnimalID
    WHERE  (dbo.txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender'))
    AND (dbo.txnVisit.tin_DateCreated >= '2017-01-01 ')
    AND (dbo.txnVisit.tin_DateCreated < GETDATE())
    AND dbo.AnimalDetails.DateOfBirth IS NOT NULL
    --Exclude AnimlID in first numerator table
    AND dbo.Animal.AnimalID NOT IN (SELECT DISTINCT
      dbo.Animal.AnimalID
    FROM dbo.Animal
    INNER JOIN dbo.refSpecies
      ON dbo.Animal.SpeciesID = dbo.refSpecies.SpeciesID
    INNER JOIN dbo.AnimalDetails
      ON dbo.Animal.AnimalID = dbo.AnimalDetails.AnimalID
    INNER JOIN dbo.txnVisit
      ON dbo.Animal.AnimalID = dbo.txnVisit.AnimalID
    WHERE dbo.txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender')
    AND dbo.txnVisit.OutComeType IN ('Died', 'PreEuthanasia')
    AND dbo.txnVisit.tin_DateCreated Between '2017-01-01' AND '{last_date}'))


    SELECT DISTINCT
      Animalid,
      Species,
      Name,
      IntakeType,
      Dateofbirth,
      Intakedate,
      DATEDIFF(WEEK, dateofbirth, intakedate) AS IntakeAge,
      Cast('{reference_date}' as Date) as ReferenceDate,
      'Alive' AS Outcometype
    FROM Denom 
   
    UNION
    
    SELECT
      Animalid,
      Species,
      Name,
      IntakeType,
      Dateofbirth,
      Intakedate,
      DATEDIFF(WEEK, dateofbirth, intakedate) AS IntakeAge,
      Outcomedate,
      Outcometype

    FROM numerator
    WHERE outcomedate BETWEEN '{reference_date}' AND '{last_date}'
    
    '''
    #converts query to dataframe
    df=pd.read_sql(query,conn)
    #changes strings to datetime dtypes
    df[["Dateofbirth", "Intakedate", "ReferenceDate"]] = df[["Dateofbirth", "Intakedate", "ReferenceDate"]].apply(
        lambda x: pd.to_datetime(x).dt.date)
    #Remove occurences where there are 2 same agegroups for outcome type
    #E.g same agegroup for a kitten died and alive thus causing duplicate
    #Deletes Alive row for such occurences
    #df=df.drop_duplicates(subset=['Animalid','Agegroup'], keep='last')

    return df

    
    
@flow(log_prints=True)
def combined_df(start_year:int, end_year:int) -> pd.DataFrame:
    """Combines the dataframes for each year and months into a single dataframe"""
    combined_df=[]
    #iterates through every year and month and appends the dataframe to combined_df
    for year in range(start_year,end_year + 1):
        for month in range(1,13):
            #Ensures unnecessary dataframes for future months in current year are not generated
            if year == datetime.today().year and month > datetime.today().month -1:
                break
            else:
                combined_df.append(extraction(year,month))
        print("Data extracted for: "+str(year))
    #Combines all dataframes into single dataframe
    #df is denominator
    df=pd.concat(combined_df, ignore_index=True)
    #Sorting the DataFrame by referencedate and AgeGroup
    df = df.sort_values(by=['Animalid']).reset_index(drop=True)
    
    return df

@task(log_prints=True)
def save_to_excel(df: pd.DataFrame, local_path: str) -> None :
    '''Saves dataframe to Server and autofits columns'''
    #Extracts Numerator from df
    df_num=df[df['Outcometype'].isin(['Died', 'Euthanised'])]
    #Saves Sheets to Excel
    writer = pd.ExcelWriter(local_path, engine = 'xlsxwriter')
    df.to_excel(writer, header=True, index=False, sheet_name = 'Denominator')
    df_num.to_excel(writer, header=True, index=False, sheet_name = 'Numerator')
    writer.close()  
    
    #Autofit Column width
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(local_path)
    denom = wb.Worksheets("Denominator")
    num= wb.Worksheets("Numerator")
    denom.Columns.AutoFit()
    num.Columns.AutoFit()
    wb.Save()
    excel.Application.Quit()
    
    return

@task(log_prints=True)
def sharepoint_upload(local_path: str, remote_path: str):
    '''Uploads data from Server to Sharepoint'''
    siteurl = 'https://torontohumanesociety.sharepoint.com/sites/ShelterVeterinaryTeamcopy-Sheltermetrics/'
    #Uses Environment Variables to get login credentials
    username=os.environ.get("SHAREPOINT_USER")
    password=os.environ.get("SHAREPOINT_PASSWORD")
    #authentication block
    ctx_auth = AuthenticationContext(siteurl) 
    ctx_auth.acquire_token_for_user(username, password)
    ctx = ClientContext(siteurl, ctx_auth)
    #Gets target folder on sharepoint using remote path
    target_folder = ctx.web.get_folder_by_server_relative_url(remote_path)
    #Accesses file on local server
    with open(local_path, "rb") as content_file:
        try:
            file_content = content_file.read()
            uploaded=target_folder.upload_file(os.path.basename(local_path), file_content).execute_query()
        except Exception as e:
            print(e)
            
    return uploaded

@task(log_prints=True)
def update_dashboard(dashboard_path:str):
    """Updates dashboard, adds Numerator and Denominator data to Dashboard Sheet"""
    excel=win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(dashboard_path)
    #Refresh Dashboard with new data
    wb.RefreshAll()
    wb.Save()
    excel.Application.Quit()
    return

def filter_last_12_months(df):
    """
    Filters the DataFrame to include only rows where ReferenceDate is within the last 6 months.
    Also ensures missing ReferenceDate months are filled in with zero values for visualization.

    Parameters:
        df (pd.DataFrame): Input DataFrame with 'ReferenceDate' column.

    Returns:
        pd.DataFrame: Filtered DataFrame with missing months populated.
    """
    # Ensure ReferenceDate is in datetime format
    df = df.copy()  # Avoid modifying the original DataFrame
    df["ReferenceDate"] = pd.to_datetime(df["ReferenceDate"])

    # Get today's date normalized to midnight
    today = pd.Timestamp.today().normalize()

    # First day of the current month
    max_date = today.replace(day=1)

    # Start date = first day of the month 12 months ago
    start_date = max_date - relativedelta(months=13)

    # Filter SurgeryDate between start_date (inclusive) and max_date (exclusive)
    mask = (df["ReferenceDate"] >= start_date) & (df["ReferenceDate"] < max_date)
    df = df.loc[mask]

    # Add a count column
    df["sum"] = 1
    df.loc[df["Animalid"].isna(), "sum"] = 0
    df["Month"] = df["ReferenceDate"].dt.to_period("M")
    return df

def process_bi_data(df):
    # Replace NaN in 'sum' where 'Animalid' is missing
    df.loc[df["Animalid"].isna(), "sum"] = 0

    # Subset relevant columns
    df = df[["ReferenceDate", "Outcometype", "Agegroup", "sum"]].copy()

    # Classify outcomes into 'alive' and 'deceased'
    df["outcome_refined"] = np.where(df["Outcometype"] == "Alive", "alive", "deceased")

    # Normalize 'ReferenceDate' and extract monthly period
    df["ReferenceDate"] = pd.to_datetime(df["ReferenceDate"])
    df["Month"] = df["ReferenceDate"].dt.to_period("M")

    # Segment dataset by age group categories
    df_above_2 = df[df["Agegroup"].isin(["7-12 wks", "3-6 wks"])]
    df_below_2 = df[df["Agegroup"] == "0-2 wks"]

    # Internal function to compute deceased percentage and assign category label
    def compute_percentage(df_subset, category_label):
        outcome_summary = (
            df_subset.groupby(["Month", "outcome_refined"])["sum"]
            .sum()
            .unstack(fill_value=0)
        )
        outcome_summary["deceased_pct"] = (
            outcome_summary.get("deceased", 0)
            / (outcome_summary.get("deceased", 0) + outcome_summary.get("alive", 0))
        ).fillna(0) * 100

        result = outcome_summary.reset_index()[["Month", "deceased_pct"]]
        result["AgeGroupCategory"] = category_label
        return result

    # Apply percentage calculation for both age groups
    below_2_result = compute_percentage(df_below_2, "0-2 wks")
    above_2_result = compute_percentage(df_above_2, "3-12 wks")

    # Concatenate results with category label
    final_result = pd.concat([below_2_result, above_2_result], ignore_index=True)
    final_result["deceased_pct"] = final_result["deceased_pct"].round(2)

    return final_result

@flow(name='Kitten Mortality Report', flow_run_name='Report for ' +str((datetime.now() - relativedelta(months=1)).strftime("%b-%Y")))
def get_kitten_report():
    """Function to run all scripts"""
    df=combined_df(start_year,end_year)
    #power_bi_df=filter_last_12_months(df)
    #power_bi_df_summary=process_bi_data(power_bi_df)
    #with pd.ExcelWriter(bi_report_path, engine='openpyxl') as writer:
      #power_bi_df.to_excel(writer, sheet_name='Sheet2', index=False)
      #power_bi_df_summary.to_excel(writer, sheet_name='Sheet1', index=False)
    save_to_excel(df, report_path)
    #update_dashboard(dashboard_path)
    #upload raw data to sharepoint
    #sharepoint_upload(report_path, remote_path)
    #upload bi data to sharepoint
    #sharepoint_upload(bi_report_path, bi_remote_path)
    #upload dashboard to sharepoint
    #sharepoint_upload(dashboard_path, remote_path)

    return


if __name__=="__main__":
    
    start_year= 2019 # datetime.today().year - 1 
    #Year report should end.
    end_year=2025 #datetime.today().year
    
    #Path to Sharepoint folder
    if datetime.today().month-1==0:
      remote_path= os.environ.get("KITTEN_REMOTE_PATH") + str(end_year-1)
    else:
      remote_path= os.environ.get("KITTEN_REMOTE_PATH") + str(end_year)
    #Path to report on local server
    report_filename="all_mortality.xlsx"
    report_path=os.environ.get("KITTEN_SERVER_PATH")+ report_filename
    #Path to power_bi_data report on local server
    bi_report_filename="kitten_mortality_bi.xlsx"
    bi_report_path=os.environ.get("KITTEN_SERVER_PATH")+ bi_report_filename
    bi_remote_path=os.environ.get("BI_REMOTE_PATH")
    #dashboard_path
    dashboard_filename="Kitten_Mortality_DashBoard.xlsx"
    dashboard_path=os.environ.get("KITTEN_SERVER_PATH")+ dashboard_filename
    #Year report should start from
    get_kitten_report()