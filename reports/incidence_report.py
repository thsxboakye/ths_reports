import pandas as pd
from datetime import datetime
import calendar
import os
import numpy as np
from itertools import product
from dateutil.relativedelta import relativedelta
from database.ms_sql_connection import fetch_query
from utils.utils import save_to_excel, update_dashboard, combined_df
from environment.settings import config

def denominator_extraction(year: int, month: int) -> pd.DataFrame:
    """Extraction and Transformation script from SQL"""
    #Returns Last day of the month
    #Returns Last date of the month in the format YYYY-MM-DD
    last_date=datetime.strptime(f'{year}-{month:02}-{calendar.monthrange(year, month)[1]}', '%Y-%m-%d')
    #Reference date
    reference_date=f'{year}-{month:02}-01'
    
    query = f'''
    SELECT TOP (100) PERCENT 
        dbo.Animal.AnimalID, dbo.Animal.Name, 
        CASE WHEN dbo.refSpecies.Species = 'Cat' THEN 'Cat' 
        WHEN dbo.refSpecies.Species = 'Dog' THEN 'Dog' 
        WHEN dbo.refSpecies.Species NOT IN ('Dog', 'Cat') 
        THEN 'Special Species' END AS Species, 
        CASE WHEN dbo.Animal.Sex = 'U' AND dbo.ExamTreatment.Medication = 'Ovariohysterectomy' THEN 'F' 
        WHEN dbo.Animal.Sex = 'U' AND dbo.ExamTreatment.Medication = 'Orchidectomy' THEN 'M' 
        WHEN dbo.Animal.Sex = 'F' THEN 'F' WHEN dbo.Animal.Sex = 'M' THEN 'M' ELSE dbo.Animal.Sex END AS Sex, 
        dbo.AnimalDetails.DateOfBirth, dbo.ExamTreatment.ExamTreatmentID AS UniqueSurgeryID, 
        dbo.ExamTreatment.ExamID AS SurgeryID, dbo.ExamTreatment.Medication AS SurgeryType, 
        CASE WHEN dbo.ExamTreatment.Medication IN ('Dental Dehiscence Repair', 'Dental Extraction', 
        'Dental COHAT (Lv 1-3)', 'Dental COHAT (Lv 4-5)', 'Dental Extraction, Difficult') THEN 'Dental' 
        WHEN dbo.ExamTreatment.Medication IN ('Orchidectomy', 'Orchidectomy, cryptorchid', 
        'Orchidectomy Intra-Ab Crytorch', 'Orchidectomy Inguinal') 
        THEN 'Neuter' 
        WHEN dbo.ExamTreatment.Medication IN ('Ovariohysterectomy') THEN 'Spay' 
        ELSE 'Other' END AS SurgeryCategory, 
        dbo.ExamTreatment.StatusDateTime AS SurgeryDate, 
        ROW_NUMBER() over (PARTITION by dbo.Animal.AnimalID, dbo.ExamTreatment.Medication ORDER BY dbo.ExamTreatment.StatusDateTime ASC) AS Rank,
        Person_1.NameFirst + N' ' + Person_1.NameLast AS SurgeonName, 
        dbo.Person.NameFirst + N' ' + dbo.Person.NameLast AS AssistantName, 
        CASE WHEN SiteName IN ('Toronto Humane Society', 'Toronto Humane Society Adoption Centre') 
        THEN 'THS' 
        WHEN SiteName IN ('Toronto Humane Society Public Veterinary Services', 
        'Toronto Humane Society Spay Neuter Services', 
        'Toronto Humane Society Spay Neuter Services - (HSDR)') THEN 'PVS' END AS Site, 
        dbo.Site.SiteName, dbo.refLocations.Location, dbo.HistoryLocation.LastUpdated AS LocationDate
    FROM  dbo.Person RIGHT OUTER JOIN
           dbo.ExamTreatment INNER JOIN
           dbo.Animal INNER JOIN
           dbo.refSpecies ON dbo.Animal.SpeciesID = dbo.refSpecies.SpeciesID INNER JOIN
           dbo.AnimalDetails ON dbo.Animal.AnimalID = dbo.AnimalDetails.AnimalID 
           ON dbo.ExamTreatment.AnimalID = dbo.Animal.AnimalID INNER JOIN
           dbo.Site ON dbo.ExamTreatment.SiteID = dbo.Site.SiteID LEFT OUTER JOIN
           dbo.refLocations INNER JOIN
           dbo.HistoryLocation ON dbo.refLocations.LocationID = dbo.HistoryLocation.LocationID 
           ON dbo.Animal.AnimalID = dbo.HistoryLocation.AnimalID LEFT OUTER JOIN
           dbo.Person AS Person_1 ON dbo.ExamTreatment.PerformedBy = Person_1.PersonID 
           ON dbo.ExamTreatment.AssistantID = dbo.Person.PersonID
    WHERE (dbo.ExamTreatment.Medication IN ('Urethrostomy, Perineal', 'Amputation, Digit', 
        'Amputation, Tail', 'Orchidectomy Intra-Ab Crytorch', 'Orchidectomy Inguinal', 
        'Amputation, Hind Leg', 'Laceration (minor)', N'Rectal Prolapse Repair', N'Amputation', 
        N'Biopsy', N'Cherry eye repair', N'Cruciate Ligament Repair', N'Cystotomy', N'Dental Dehiscence Repair', 
        N'Dental Extraction', N'Dental COHAT (Lv 1-3)', N'Dental COHAT (Lv 4-5)', N'Dental Extraction, Difficult', 
        N'Ectropion correction', N'Entropion Correction', N'Enucleation', N'Esophagostomy tube placement', 
        N'Exploratory Laparotomy', 
        N'Femoral head ostectomy', N'Fracture repair', N'Hernia repair', N'Incision repair', 
        N'Mass Removal', N'Orchidectomy', N'Orchidectomy, cryptorchid', N'Ovariohysterectomy', 
        N'Patella Luxation Surgery', N'Total ear canal ablation', N'Total hip replacement', N'TPLO')) 
        AND dbo.ExamTreatment.StatusDateTime BETWEEN '{reference_date}' AND '{last_date}'
    '''
    df1 = fetch_query(query)
    #denominator_refined
    print(df1.shape)
    #Remove Off Site Animals
    df1=df1[df1['Location']!='Off Site Clinic']
    #Sort animals, keep first record and remove duplicates
    df1=df1.sort_values(['AnimalID', 'SurgeryDate', 'UniqueSurgeryID', 'SurgeryType', 'LocationDate']).reset_index(drop=True)
    df1=df1.drop_duplicates(subset=['AnimalID', 'UniqueSurgeryID'])
    #Remove Duplicate Same Surgeries that happened sameday
    df1['SurgeryDate']=pd.to_datetime(df1['SurgeryDate']).dt.date
    df1=df1.drop_duplicates(subset=['AnimalID', 'SurgeryType', 'SurgeryDate'])
    multiple=df1.groupby(['AnimalID', 'SurgeryDate', 'SurgeryID']).agg({'UniqueSurgeryID': np.size}).rename(columns={'UniqueSurgeryID': 'num_surg_mapping'}).reset_index()
    multiple=multiple[multiple['num_surg_mapping']>1]
    denominator_refined=pd.merge(df1, multiple, on=['AnimalID','SurgeryDate', 'SurgeryID'],\
                                 how='outer', indicator=True)
    denominator_refined['SurgeryCategory_refined']=np.where(denominator_refined['_merge']=='both', 'Multiple', denominator_refined['SurgeryCategory'])
    denominator_refined['num_surg_mapping']=np.where(denominator_refined['num_surg_mapping'].isnull(), 1, denominator_refined['num_surg_mapping'])
    denominator_refined[denominator_refined['SurgeryCategory_refined']=='Multiple']
    del denominator_refined['_merge']
    return denominator_refined 


def numerator_extraction(year: int, month: int) -> pd.DataFrame:
    """Extraction and Transformation script from SQL"""
    #Returns Last day of the month
    #Returns Last date of the month in the format YYYY-MM-DD
    last_date=datetime.strptime(f'{year}-{month:02}-{calendar.monthrange(year, month)[1]}', '%Y-%m-%d')
    #Reference date
    reference_date=f'{year}-{month:02}-01'

    # numerator
    query=f'''
    SELECT TOP (100) PERCENT 
        dbo.Animal.AnimalID, dbo.Animal.Name, 
        CASE WHEN dbo.refSpecies.Species = 'Cat' THEN 'Cat' 
        WHEN dbo.refSpecies.Species = 'Dog' THEN 'Dog' 
        WHEN dbo.refSpecies.Species NOT IN ('Dog', 'Cat') THEN 'Special Species' 
        END AS Species, 
        CASE WHEN dbo.Animal.Sex = 'U' AND dbo.ExamTreatment.Medication = 'Ovariohysterectomy' THEN 'F' 
        WHEN dbo.Animal.Sex = 'U' AND dbo.ExamTreatment.Medication = 'Orchidectomy' THEN 'M' 
        WHEN dbo.Animal.Sex = 'F' THEN 'F' WHEN dbo.Animal.Sex = 'M' THEN 'M' 
        ELSE dbo.Animal.Sex END AS Sex, 
        dbo.AnimalDetails.DateOfBirth, 
        dbo.ExamTreatment.ExamID AS SurgeryID, dbo.ExamTreatment.Medication AS SurgeryType, 
        CASE WHEN dbo.ExamTreatment.Medication IN ('Dental Dehiscence Repair', 'Dental Extraction', 
        'Dental COHAT (Lv 1-3)', 'Dental COHAT (Lv 4-5)', 'Dental Extraction, Difficult') THEN 'Dental' 
        WHEN dbo.ExamTreatment.Medication IN ('Orchidectomy', 'Orchidectomy, cryptorchid', 'Orchidectomy Inguinal') THEN 'Neuter' WHEN dbo.ExamTreatment.Medication IN ('Ovariohysterectomy') THEN 'Spay' ELSE 'Other' END AS SurgeryCategory, 
        dbo.ExamTreatment.StatusDateTime AS SurgeryDate, 
        dbo.refCondition.Condition AS SxComp, 
        dbo.ExamCondition.DateTimeDiagnosed AS CompDate, 
        ROW_NUMBER() over (PARTITION by dbo.Animal.AnimalID, dbo.ExamTreatment.Medication ORDER BY dbo.ExamTreatment.StatusDateTime ASC) AS Rank,
        DATEDIFF(d, dbo.ExamTreatment.DateCreated, dbo.ExamCondition.DateCreated) AS DaysAfterSurgery, 
        Person_1.NameFirst + N' ' + Person_1.NameLast AS SurgeonName, 
        dbo.Person.NameFirst + N' ' + dbo.Person.NameLast AS AssistantName, 
        CASE WHEN SiteName IN ('Toronto Humane Society', 'Toronto Humane Society Adoption Centre') 
        THEN 'THS' WHEN SiteName IN ('Toronto Humane Society Public Veterinary Services', 
        'Toronto Humane Society Spay Neuter Services', 
        'Toronto Humane Society Spay Neuter Services - (HSDR)') THEN 'PVS' END AS Site, 
        dbo.ExamCondition.ExamConditionID AS UniqueCompID, dbo.ExamCondition.ConditionID AS CompTypeID, 
        dbo.ExamTreatment.ExamTreatmentID AS UniqueSurgeryID, dbo.ExamTreatment.TreatmentID AS SurgeryTypeID, 
        dbo.refLocations.Location, dbo.HistoryLocation.LastUpdated AS LocationDate
    FROM  dbo.refLocations INNER JOIN
           dbo.HistoryLocation ON dbo.refLocations.LocationID = dbo.HistoryLocation.LocationID RIGHT OUTER JOIN
           dbo.ExamTreatment INNER JOIN
           dbo.Animal INNER JOIN
           dbo.AnimalDetails ON dbo.Animal.AnimalID = dbo.AnimalDetails.AnimalID INNER JOIN
           dbo.refSpecies ON dbo.Animal.SpeciesID = dbo.refSpecies.SpeciesID 
           ON dbo.ExamTreatment.AnimalID = dbo.Animal.AnimalID INNER JOIN
           dbo.Site ON dbo.ExamTreatment.SiteID = dbo.Site.SiteID INNER JOIN
           dbo.ExamCondition ON dbo.Animal.AnimalID = dbo.ExamCondition.AnimalID INNER JOIN
           dbo.refCondition ON dbo.ExamCondition.ConditionID = dbo.refCondition.ConditionID 
           ON dbo.HistoryLocation.AnimalID = dbo.Animal.AnimalID LEFT OUTER JOIN
           dbo.Person AS Person_1 ON dbo.ExamTreatment.PerformedBy = Person_1.PersonID LEFT OUTER JOIN
           dbo.Person ON dbo.ExamTreatment.AssistantID = dbo.Person.PersonID
    WHERE 
        (dbo.refCondition.Condition IN ('Incision complications', 'Surgical complication', 'Anesthetic complication', 'Anesthetic arrest')) 
        AND (dbo.ExamTreatment.Medication IN ('Urethrostomy, Perineal', 'Amputation, Digit', 
        'Amputation, Tail', 'Orchidectomy Intra-Ab Crytorch', 'Orchidectomy Inguinal', 
        'Amputation, Hind Leg', 'Laceration (minor)', N'Rectal Prolapse Repair', N'Amputation', 
        N'Biopsy', N'Cherry eye repair', N'Cruciate Ligament Repair', N'Cystotomy', 
        N'Dental Dehiscence Repair', N'Dental Extraction', N'Dental COHAT (Lv 1-3)', 
        N'Dental COHAT (Lv 4-5)', N'Dental Extraction, Difficult', N'Ectropion correction', 
        N'Entropion Correction', 'COHAT', N'Enucleation', N'Esophagostomy tube placement', 
        N'Exploratory Laparotomy', N'Femoral head ostectomy', N'Fracture repair', 
        N'Hernia repair', N'Incision repair', N'Mass Removal', N'Orchidectomy', 
        N'Orchidectomy, cryptorchid', N'Ovariohysterectomy', N'Patella Luxation Surgery', 
        N'Total ear canal ablation', N'Total hip replacement', N'TPLO')) AND 
        (DATEDIFF(d, dbo.ExamTreatment.DateCreated, dbo.ExamCondition.DateCreated) >= - 2) 
        AND dbo.ExamTreatment.StatusDateTime BETWEEN '{reference_date}' AND '{last_date}'
        AND dbo.ExamCondition.DateTimeDiagnosed BETWEEN '{reference_date}' AND '{last_date}'
         
    '''
    df2 = fetch_query(query)
    #Remove Off Site Animals
    df2=df2[df2['Location']!='Off Site Clinic']
    #df2=df2[df2['SurgeryType']!='Dental Dehiscence Repair']
    #df2=df2[df2['SurgeryCategory']!='Dental']
    #Sort animals, keep first record and remove duplicates
    df2=df2.sort_values(['AnimalID','UniqueSurgeryID', 'UniqueCompID']).reset_index(drop=True)
    df2=df2.drop_duplicates(subset=['AnimalID','UniqueSurgeryID', 'UniqueCompID'])
    #Remove Duplicates
    df2['SurgeryDate']=pd.to_datetime(df2['SurgeryDate']).dt.date
    df2=df2.drop_duplicates(subset=['AnimalID','SurgeryType', 'SxComp'])
    df2=df2.sort_values(['AnimalID', 'SurgeryType','SxComp','SurgeryDate', 'CompDate'])
    numerator = df2.drop(['CompTypeID','SurgeryTypeID' ], axis=1)
    #get the complication that is the min days after surgery
    numerator_refined=numerator.groupby(['AnimalID', 'SxComp']).agg({'DaysAfterSurgery': pd.Series.min}).reset_index()
    numerator_refined=pd.merge(numerator_refined, numerator, on=['AnimalID', 'SxComp', 'DaysAfterSurgery'], how='inner')
    numerator_refined['SurgeryDate']=pd.to_datetime(numerator_refined['SurgeryDate']).dt.date
    numerator_refined['CompDate']=pd.to_datetime(numerator_refined['CompDate']).dt.date
    #create the multiple category for if there were multiple surgeries for a given complicaiton
    multiple=numerator_refined.groupby(['AnimalID', 'SxComp', 'SurgeryDate']).agg({'SurgeryID': np.size}).rename(columns={'SurgeryID': 'num_surg_mapping'}).reset_index()
    multiple=multiple[multiple['num_surg_mapping']>1]
    numerator_refined=pd.merge(numerator_refined, multiple, on=['AnimalID', 'SxComp', 'SurgeryDate'], how='outer', indicator=True)
    numerator_refined['SurgeryCategory_refined']=np.where(numerator_refined['_merge']=='both', 'Multiple', numerator_refined['SurgeryCategory'])
    numerator_refined['num_surg_mapping']=np.where(numerator_refined['num_surg_mapping'].isnull(), 1, numerator_refined['num_surg_mapping'])
    #Remove unwanted columns
    numerator_refined = numerator_refined.drop(['num_surg_mapping', 'Location', 'LocationDate', '_merge'], axis=1)

    return numerator_refined


def create_dashboard_data(numerator, denominator, path: str) -> None :
    '''Saves dataframe to Server and autofits columns'''
    #Saves Sheets to Excel
    writer = pd.ExcelWriter(path, engine = 'xlsxwriter')
    numerator=numerator[['AnimalID', 'Name', 'Species', 'Sex','SurgeryType', 'SurgeryCategory',
       'SurgeryDate', 'SurgeryCategory_refined', 'SurgeonName']]
    denominator=denominator[['AnimalID', 'Name', 'Species', 'Sex','SurgeryType', 'SurgeryCategory',
        'SurgeryDate', 'SurgeryCategory_refined', 'SurgeonName']]
    numerator["Type"]='Comp'
    denominator["Type"]='NoComp'
    merged_data=pd.concat([denominator, numerator], ignore_index=True)
    # Convert SurgeryDate to datetime if it's not already
    merged_data['SurgeryDate'] = pd.to_datetime(merged_data['SurgeryDate'], errors='coerce')
    # Creating SurgeryMonth column: extracting Year and Month from SurgeryDate
    merged_data['SurgeryMonth'] = merged_data['SurgeryDate'].dt.to_period('M')
    # Remove duplicates, prioritizing 'Comp' rows over 'NoComp' for each 'AnimalID' and 'SurgeryMonth'
    merged_data = merged_data.sort_values(by=['AnimalID', 'SurgeryMonth','SurgeryCategory', 'Type'], ascending=[True, True, True, False])
    merged_data = merged_data.drop_duplicates(subset=['AnimalID', 'SurgeryMonth', 'SurgeryCategory'], keep='last')
    merged_data.to_excel(writer, header=True, index=False, sheet_name = 'Denominator')
    writer.close()  
    print("Report file saved to server.")
    return merged_data



def filter_last_12_months(df):
    """
    Filters the DataFrame to include only rows where SurgeryDate is within the last 12 full months,
    counting back from the 1st day of the current month.

    Parameters:
        df (pd.DataFrame): Input DataFrame with 'SurgeryDate' column.

    Returns:
        pd.DataFrame: Filtered DataFrame with only the last 12 full months of data.
    """
    # Ensure SurgeryDate is datetime
    df["SurgeryDate"] = pd.to_datetime(df["SurgeryDate"])

    # Get today's date normalized to midnight
    today = pd.Timestamp.today().normalize()

    # First day of the current month
    max_date = today.replace(day=1)

    # Start date = first day of the month 12 months ago
    start_date = max_date - relativedelta(months=13)

    # Filter SurgeryDate between start_date (inclusive) and max_date (exclusive)
    mask = (df["SurgeryDate"] >= start_date) & (df["SurgeryDate"] < max_date)
    filtered_df = df.loc[mask]

    return filtered_df


def process_bi_data(df):
    """
    Filters and processes surgery data to compute the percentage distribution of 'Type'
    for spay and neuter surgeries, grouped by SurgeryCategory_refined, SurgeryMonth, and SpeciesGroup.
    Missing months are filled with 0 percentages.

    Parameters:
        df (pd.DataFrame): Input DataFrame with required columns.

    Returns:
        pd.DataFrame: Combined DataFrame with all group combinations and 0s for missing ones.
    """
    # Select relevant columns
    df = df[["Species", "SurgeryCategory_refined", "SurgeryMonth", "Type"]]

    # Keep only Spay and Neuter surgeries
    df = df[df["SurgeryCategory_refined"].isin(["Spay", "Neuter"])]

    # Create species group
    df["SpeciesGroup"] = np.where(df["Species"] == "Special Species", "special", "cat_dog")

    # Ensure SurgeryMonth is a string in YYYY-MM format
    #df["SurgeryMonth"] = pd.to_datetime(df["SurgeryMonth"]).dt.strftime('%Y-%m')

    # Get all unique values needed for combinations
    all_months = sorted(df["SurgeryMonth"].unique())
    all_groups = df["SpeciesGroup"].unique()
    all_categories = df["SurgeryCategory_refined"].unique()
    all_types = df["Type"].unique()

    # Create full cartesian product
    full_index = pd.DataFrame(
        list(product(all_groups, all_categories, all_months, all_types)),
        columns=["SpeciesGroup", "SurgeryCategory_refined", "SurgeryMonth", "Type"]
    )

    # Compute percentages
    grouped = (
        df.groupby(["SpeciesGroup", "SurgeryCategory_refined", "SurgeryMonth"])["Type"]
        .value_counts(normalize=True)
        .rename("Percentage")
        .mul(100)
        .reset_index()
    )

    # Merge with full index and fill missing values with 0
    final_df = (
        full_index.merge(grouped, on=["SpeciesGroup", "SurgeryCategory_refined", "SurgeryMonth", "Type"], how="left")
        .fillna({"Percentage": 0})
        .sort_values(["SpeciesGroup", "SurgeryCategory_refined", "SurgeryMonth", "Type"])
        .reset_index(drop=True)
    )
    # Round percentages to 1 decimal place
    final_df["Percentage"] = final_df["Percentage"].round(1)

    return final_df




def run_incidence_report(report_year):
    """Function to run all scripts"""
    numerator=combined_df(numerator_extraction, report_year-1,report_year)
    denominator=combined_df(denominator_extraction, report_year-1,report_year)
    dashboard_data=create_dashboard_data(numerator, denominator, dashboard_data_path)
    bi_data=filter_last_12_months(dashboard_data)
    bi_data=process_bi_data(bi_data)
    bi_data.to_excel(bi_report_path,index=False)
    save_to_excel(numerator, denominator, report_path)  
    update_dashboard(dashboard_path)

    return


#start year
start_year=datetime.today().year -1
#Year report should end.
end_year=datetime.today().year
                
#Path to report on local server
report_filename="incidence_complication.xlsx"
report_path=f"{config.SERVER_PATH}/sxcomp/{report_filename}"

#Path to power_bi_data report on local server
bi_report_filename="sx_comp_bi_report.xlsx"
bi_report_path=f"{config.SERVER_PATH}/power_bi/{bi_report_filename}"
#dashboard_path
dashboard_filename="SurgicalComplicationsDashboard.xlsx"
dashboard_path=f"{config.SERVER_PATH}/sxcomp/{dashboard_filename}"
dashboard_file_data="incidence_complication_data.xlsx"
dashboard_data_path=f"{config.SERVER_PATH}/sxcomp/{dashboard_file_data}"