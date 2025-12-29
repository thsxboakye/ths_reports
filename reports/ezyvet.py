import pandas as pd
import win32com.client as win32
from dotenv import load_dotenv
import os
import pythoncom
import os
import glob
from dateutil.relativedelta import relativedelta
from itertools import product
import numpy as np
from utils.utils import update_dashboard
from environment.settings import config


def rename_files():
    # Define the patterns for the filenames
    invoice_pattern = 'Invoice Lines-*.csv'
    new_invoice_filename = 'Invoice.csv'
    animal_pattern = 'Animals-*.csv'
    new_animal_filename = 'Animals.csv'

    # Rename invoice files
    invoice_files = glob.glob(invoice_pattern)
    if invoice_files:
        for old_filename in invoice_files:
            try:
                os.rename(old_filename, new_invoice_filename)
                print(f'Renamed file from {old_filename} to {new_invoice_filename}')
            except FileNotFoundError:
                print(f'Error: The file {old_filename} does not exist.')
            except PermissionError:
                print('Error: Permission denied. You do not have permission to rename this file.')
            except Exception as e:
                print(f'An unexpected error occurred: {e}')
    else:
        print('No invoice files matching the pattern were found.')

    # Rename animal files
    animal_files = glob.glob(animal_pattern)
    if animal_files:
        for old_filename in animal_files:
            try:
                os.rename(old_filename, new_animal_filename)
                print(f'Renamed file from {old_filename} to {new_animal_filename}')
            except FileNotFoundError:
                print(f'Error: The file {old_filename} does not exist.')
            except PermissionError:
                print('Error: Permission denied. You do not have permission to rename this file.')
            except Exception as e:
                print(f'An unexpected error occurred: {e}')
    else:
        print('No animal files matching the pattern were found.')


def extraction(animal_path, invoice_path) -> pd.DataFrame:
    #Animal records dataframe
    dfa=pd.read_csv(animal_path)
    print(1)
    dfa=dfa[['Animal Code','Animal Name', 'Species', 'Owner First Name', 'Owner Last Name', 'Master Problems', 'Last Visit']]
    print(2)
    #exclude all TNR
    dfa=dfa[~dfa['Animal Name'].str.contains('^Queensv.*', na=False)]
    dfa=dfa[~dfa['Animal Name'].str.contains('^TNR.*', na=False)]
    dfa=dfa[~dfa['Animal Name'].str.contains('TNR.*', na=False)]
    #Remove all animal records with no master problems
    print(3)
    dfa.dropna(subset=['Master Problems'], inplace=True)
    dfa.dtypes
    #Invoice records dataframe
    df=pd.read_csv(invoice_path)
    print(2)
    df=df[['Invoice Date','Department', 'Business Name', 'Animal Code', 'Patient Name', 'Species','Product Name','Staff Member', 'Case Owner']]
    print(2)
    df['Invoice Date']=pd.to_datetime(df['Invoice Date'])
    df=df[~df['Patient Name'].str.contains('^TNR.*', na=False)]
    df=df[~df['Patient Name'].str.contains('TNR.*', na=False)]
    print(4)
    #If a staff member row is Null, we fill with values from Case owner if available
    df['Staff Member'] = df['Staff Member'].fillna(df['Case Owner'])
    print(4)
    #drop all null staff member records
    
    df=df.dropna(subset=['Staff Member'])
    
    #Remove case owner column
    df=df.drop('Case Owner', axis=1)
    df['SurgeryType']='Other'
    df.loc[df['Product Name'].str.contains('Spay.*'), 'SurgeryType']='Spay'
    df.loc[df['Product Name'].str.contains('COHAT.*'), 'SurgeryType']='Dental'
    df.loc[df['Product Name'].str.contains('Neuter.*'), 'SurgeryType']='Neuter'
    #Exclude all other Product Names
    df=df[df['SurgeryType']!='Other']
    df=df[df['Staff Member']!='Ezy Support']
    df=df[df['Business Name']!='ezyVet Software Support']
    #Remove duplicate records with Same surgery
    df = df[~df.duplicated(subset=['Animal Code', 'SurgeryType'])]
    #convert int to string
    df['Animal Code'] = df['Animal Code'].astype(str)
    dff=df.merge(dfa[['Animal Code', 'Master Problems']], on='Animal Code', how='left')
    dff['Master Problems']=dff['Master Problems'].fillna('No Complications')
    dff = dff.rename(columns={'Invoice Date': 'Date', 'Staff Member':'All Resources / Vets'})
    #Classify master problems
    dff['Comp']='Others'
    dff.loc[dff['Master Problems']=='Surgical complication', 'Comp']='SIC'
    dff.loc[dff['Master Problems']=='Incision complications', 'Comp']='SIC'
    dff.loc[dff['Master Problems']=='Incision complications', 'Comp']='SIC'
    dff.loc[dff['Master Problems']=='Surgical complication,Pyometra,Dehiscence, dental,COHAT 1-2', 'Comp']='Multiple'
    dff.loc[dff['Master Problems']=='Vomiting,Surgical complication,Anesthetic arrest,Anemia','Comp']='Multiple'
    dff.loc[dff['Master Problems']=='Vomiting,Surgical complication,Arrest, anesthetic,Anemia', 'Comp']='Multiple'
    dff.loc[dff['Master Problems']=='Anesthetic complication', 'Comp']='Anest'
    dff.loc[dff['Master Problems']=='No Complications', 'Comp']='No Comp'
    #removes all TNR
    dff=dff[~dff['Patient Name'].str.contains('tnr|tnR|tNr|tNR|Tnr|TnR|TNr|TNR*', na=False)]
    dff=dff[~dff['Product Name'].str.contains('tnr|tnR|tNr|tNR|Tnr|TnR|TNr|TNR*', na=False)]
    dff=dff[~dff['Product Name'].str.contains('Pre-Anesthetic*', na=False)]

    return dff


def save_to_excel(df: pd.DataFrame, local_path: str) -> None :
    '''Saves dataframe to Server and autofits columns'''
    #Extracts Numerator from df
    df_num=df[df['Master Problems']!='No Complications']
    #Saves Sheets to Excel
    writer = pd.ExcelWriter(local_path, engine = 'xlsxwriter')
    df.to_excel(writer, header=True, index=False, sheet_name = 'Denominator')
    df_num.to_excel(writer, header=True, index=False, sheet_name = 'Numerator')
    writer.close()  
    
    #Autofit Column width
    pythoncom.CoInitialize()
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(local_path)
    denom = wb.Worksheets("Denominator")
    num= wb.Worksheets("Numerator")
    denom.Columns.AutoFit()
    num.Columns.AutoFit()
    wb.Save()
    excel.Application.Quit()
    pythoncom.CoUninitialize()
    
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
    df["Date"] = pd.to_datetime(df["Date"])
    # Get today's date normalized to midnight
    today = pd.Timestamp.today().normalize()

    # First day of the current month
    max_date = today.replace(day=1)

    # Start date = first day of the month 12 months ago
    start_date = max_date - relativedelta(months=13)

    # Filter SurgeryDate between start_date (inclusive) and max_date (exclusive)
    mask = (df["Date"] >= start_date) & (df["Date"] < max_date)
    df = df.loc[mask]


    return df

def process_bi_data(df):
    """
    Filters and processes surgery data to compute the percentage distribution of 'Type'
    for spay and neuter surgeries, grouped by SurgerType, SurgeryMonth, and SpeciesGroup.
    Missing months are filled with 0 percentages.

    Parameters:
        df (pd.DataFrame): Input DataFrame with required columns.

    Returns:
        pd.DataFrame: Combined DataFrame with all group combinations and 0s for missing ones.
    """
    #Create Month Column
    df['SurgeryMonth'] = df['Date'].dt.to_period('M')
    #create Comp Type Column
    df["Type"] = df["Comp"].apply(lambda x: "No Comp" if x == "No Comp" else "Comp")
    # Select relevant columns
    df = df[["Species", "SurgeryType", "SurgeryMonth", "Type"]]

    # Keep only Spay and Neuter surgeries
    df = df[df["SurgeryType"].isin(["Spay", "Neuter"])]

    # Create species group
    df["SpeciesGroup"] = np.where(df["Species"] == "Special Species", "special", "cat_dog")

    # Ensure SurgeryMonth is a string in YYYY-MM format
    #df["SurgeryMonth"] = pd.to_datetime(df["SurgeryMonth"]).dt.strftime('%Y-%m')

    # Get all unique values needed for combinations
    all_months = sorted(df["SurgeryMonth"].unique())
    all_groups = df["SpeciesGroup"].unique()
    all_categories = df["SurgeryType"].unique()
    all_types = df["Type"].unique()

    # Create full cartesian product
    full_index = pd.DataFrame(
        list(product(all_groups, all_categories, all_months, all_types)),
        columns=["SpeciesGroup", "SurgeryType", "SurgeryMonth", "Type"]
    )

    # Compute percentages
    grouped = (
        df.groupby(["SpeciesGroup", "SurgeryType", "SurgeryMonth"])["Type"]
        .value_counts(normalize=True)
        .rename("Percentage")
        .mul(100)
        .reset_index()
    )

    # Merge with full index and fill missing values with 0
    final_df = (
        full_index.merge(grouped, on=["SpeciesGroup", "SurgeryType", "SurgeryMonth", "Type"], how="left")
        .fillna({"Percentage": 0})
        .sort_values(["SpeciesGroup", "SurgeryType", "SurgeryMonth", "Type"])
        .reset_index(drop=True)
    )
    # Round percentages to 1 decimal place
    final_df["Percentage"] = final_df["Percentage"].round(1)

    return final_df


def get_ezyvet_report():
    """Function to run all scripts"""
    #rename_files()
    df=extraction(animal_path, invoice_path)
    save_to_excel(df, report_path)
    bi_raw_data=filter_last_12_months(df)
    bi_data=process_bi_data(bi_raw_data)
    bi_data.to_excel(bi_path,index=False)
    #os.remove(animal_path)
    #os.remove(invoice_path)
    update_dashboard(dashboard_path)
    #upload raw data to sharepoint
    #sharepoint_upload(report_path, remote_path)
    #upload dashboard to sharepoint
    #sharepoint_upload(dashboard_path, remote_path)

    return


animal_path=f"{config.SERVER_PATH}/ezyvet/Animals.csv"
invoice_path=f"{config.SERVER_PATH}/ezyvet/Invoice.csv"
report_filename="ezyvet_report.xlsx"
report_path=f"{config.SERVER_PATH}/ezyvet/{report_filename}"
#bi path
bi_report_filename="bi_ezyvet_report.xlsx"
bi_path=f"{config.SERVER_PATH}/power_bi/{bi_report_filename}"
#dashboard_path
dashboard_filename="ezyvet_DashBoard.xlsx"
dashboard_path=f"{config.SERVER_PATH}/ezyvet/{dashboard_filename}"
    