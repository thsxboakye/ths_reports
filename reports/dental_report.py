import pandas as pd
from datetime import datetime
import calendar
import numpy as np
from utils.utils import save_to_excel, update_dashboard, combined_df
from database.ms_sql_connection import fetch_query
from environment.settings import config

def denominator_extraction(year: int, month: int) -> pd.DataFrame:
    """Extraction and Transformation script from SQL"""
    #Returns Last day of the month
    #Returns Last date of the month in the format YYYY-MM-DD
    last_date=datetime.strptime(f'{year}-{month:02}-{calendar.monthrange(year, month)[1]}', '%Y-%m-%d')
    #Reference date
    reference_date=f'{year}-{month:02}-01'
    
    query=f'''

            SELECT TOP (100) PERCENT 
            dbo.Animal.AnimalID, dbo.Animal.Name, dbo.refSpecies.Species, 
            dbo.Animal.Sex, dbo.AnimalDetails.DateOfBirth, 
            dbo.ExamTreatment.ExamTreatmentID AS UniqueSurgeryID, 
            dbo.ExamTreatment.ExamID AS SurgeryID, dbo.ExamTreatment.Medication AS SurgeryType, 
            CASE WHEN dbo.ExamTreatment.Medication = 'Dental Dehiscence Repair' OR
            dbo.ExamTreatment.Medication = 'Dental Extraction' OR
            dbo.ExamTreatment.Medication = 'Dental COHAT (Lv 1-3)' OR
            dbo.ExamTreatment.Medication = 'Dental COHAT (Lv 4-5)' OR
            dbo.ExamTreatment.Medication = 'Dental Extraction, Difficult' OR
            dbo.ExamTreatment.Medication = 'COHAT' THEN 'Dental' ELSE 'Other' END AS SurgeryCategory, 
            dbo.ExamTreatment.StatusDateTime AS SurgeryDate, 
            ROW_NUMBER() over (PARTITION by dbo.Animal.AnimalID, dbo.ExamTreatment.Medication ORDER BY dbo.ExamTreatment.StatusDateTime ASC) AS Rank,
            Person_1.NameFirst + N' ' + Person_1.NameLast AS SurgeonName, 
            dbo.Person.NameFirst + N' ' + dbo.Person.NameLast AS AssistantName, 
            CASE WHEN SiteName = 'Toronto Humane Society' OR
            SiteName = 'Toronto Humane Society Adoption Centre' THEN 'THS' 
            WHEN SiteName = 'Toronto Humane Society Public Veterinary Services' OR
            SiteName = 'Toronto Humane Society Spay Neuter Services' OR
            SiteName = 'Toronto Humane Society Spay Neuter Services - (HSDR)' THEN 'PVS' END AS Site, 
            dbo.refLocations.Location, dbo.HistoryLocation.LastUpdated AS LocationDate
        FROM  dbo.refLocations INNER JOIN
               dbo.HistoryLocation ON dbo.refLocations.LocationID = dbo.HistoryLocation.LocationID RIGHT OUTER JOIN
               dbo.ExamTreatment INNER JOIN
               dbo.Animal INNER JOIN
               dbo.refSpecies ON dbo.Animal.SpeciesID = dbo.refSpecies.SpeciesID INNER JOIN
               dbo.AnimalDetails ON dbo.Animal.AnimalID = dbo.AnimalDetails.AnimalID 
               ON dbo.ExamTreatment.AnimalID = dbo.Animal.AnimalID INNER JOIN
               dbo.Site ON dbo.ExamTreatment.SiteID = dbo.Site.SiteID 
               ON dbo.HistoryLocation.AnimalID = dbo.Animal.AnimalID LEFT OUTER JOIN
               dbo.Person AS Person_1 ON dbo.ExamTreatment.PerformedBy = Person_1.PersonID LEFT OUTER JOIN
               dbo.Person ON dbo.ExamTreatment.AssistantID = dbo.Person.PersonID
        WHERE 
            (dbo.ExamTreatment.Medication IN (N'Dental Dehiscence Repair', N'Dental Extraction', 
            N'Dental COHAT (Lv 1-3)', N'Dental COHAT (Lv 4-5)', N'Dental Extraction, Difficult', 'COHAT')) 
            AND dbo.ExamTreatment.StatusDateTime BETWEEN '{reference_date}' AND '{last_date}'

    '''
    df1 = fetch_query(query)
    print(df1.shape)
    #Remove Off Site Animals
    df1=df1[df1['Location']!='Off Site Clinic']
    #Remove Dehiscense
    df1=df1[df1['SurgeryType']!='Dental Dehiscence Repair']
    #Sort animals, keep first record and remove duplicates
    df1=df1.sort_values(['AnimalID', 'SurgeryDate', 'UniqueSurgeryID', 'SurgeryType', 'LocationDate']).reset_index(drop=True)
    df1=df1.drop_duplicates(subset=['AnimalID', 'UniqueSurgeryID'])
    #Remove Duplicate Same Surgeries that happened sameday
    df1['SurgeryDate']=pd.to_datetime(df1['SurgeryDate']).dt.date
    df1=df1.drop_duplicates(subset=['AnimalID', 'SurgeryType', 'SurgeryDate'])
    #Find Surgeries that happened sameday
    multiple=df1.groupby(['AnimalID', 'SurgeryDate', 'SurgeryID']).agg({'UniqueSurgeryID': np.size}).rename(columns={'UniqueSurgeryID': 'num_surg_mapping'}).reset_index()
    multiple=multiple[multiple['num_surg_mapping']>1]
    #Merge with original dataset
    denominator_refined=pd.merge(df1, multiple, on=['AnimalID','SurgeryDate', 'SurgeryID'],\
                                 how='outer', indicator=True)
    #Assign 'Multiple' as surgeryCategory to columns where animal had multiple surgeries in same day
    denominator_refined['SurgeryCategory_refined']=np.where(denominator_refined['_merge']=='both', 'Multiple', denominator_refined['SurgeryCategory'])
    #Remove unwanted columns
    denominator_refined = denominator_refined.drop(['num_surg_mapping', 'Location', 'LocationDate', '_merge'], axis=1)
    #Keep only 1 Record for Multiple Category
    multiple=denominator_refined[denominator_refined['SurgeryCategory_refined']=='Multiple']
    multiple=multiple.drop_duplicates(subset=['AnimalID', 'SurgeryDate'])
    #Rename Multiple category to Dental
    multiple['SurgeryCategory_refined']='Dental'
    non_multiple=denominator_refined[denominator_refined['SurgeryCategory_refined']!='Multiple']
    denominator_final=pd.concat([non_multiple, multiple])
    denominator_final=denominator_final.reset_index()

    return denominator_final 


def numerator_extraction(year: int, month: int) -> pd.DataFrame:
    """Extraction and Transformation script from SQL"""
    #Returns Last day of the month
    #Returns Last date of the month in the format YYYY-MM-DD
    last_date=datetime.strptime(f'{year}-{month:02}-{calendar.monthrange(year, month)[1]}', '%Y-%m-%d')
    #Reference date
    reference_date=f'{year}-{month:02}-01'

    # numerator
    query = f"""
    SELECT TOP (100) PERCENT 
        dbo.Animal.AnimalID, dbo.Animal.Name, dbo.refSpecies.Species, dbo.Animal.Sex, 
        dbo.AnimalDetails.DateOfBirth, dbo.ExamTreatment.ExamID AS SurgeryID, 
        dbo.ExamTreatment.Medication AS SurgeryType, 
        CASE WHEN dbo.ExamTreatment.Medication = 'Dental Dehiscence Repair' OR
        dbo.ExamTreatment.Medication = 'Dental Extraction' OR
        dbo.ExamTreatment.Medication = 'Dental COHAT (Lv 1-3)' OR
        dbo.ExamTreatment.Medication = 'Dental COHAT (Lv 4-5)' OR
        dbo.ExamTreatment.Medication = 'Dental Extraction, Difficult' OR 
        dbo.ExamTreatment.Medication = 'COHAT' THEN 'Dental' ELSE 'Other' END AS SurgeryCategory, 
        dbo.ExamTreatment.StatusDateTime AS SurgeryDate, 
        dbo.refCondition.Condition AS SxComp, dbo.ExamCondition.DateTimeDiagnosed AS CompDate, 
        ROW_NUMBER() over (PARTITION by dbo.Animal.AnimalID, dbo.ExamTreatment.Medication ORDER BY dbo.ExamTreatment.StatusDateTime ASC) AS Rank,
        DATEDIFF(d, dbo.ExamTreatment.DateCreated, dbo.ExamCondition.DateCreated) AS DaysAfterSurgery, 
        Person_1.NameFirst + N' ' + Person_1.NameLast AS SurgeonName, 
        dbo.Person.NameFirst + N' ' + dbo.Person.NameLast AS AssistantName, 
        CASE WHEN SiteName = 'Toronto Humane Society' OR
        SiteName = 'Toronto Humane Society Adoption Centre' THEN 'THS' 
        WHEN SiteName = 'Toronto Humane Society Public Veterinary Services' OR
        SiteName = 'Toronto Humane Society Spay Neuter Services' OR
        SiteName = 'Toronto Humane Society Spay Neuter Services - (HSDR)' THEN 'PVS' END AS Site, 
        dbo.ExamTreatment.ExamTreatmentID AS UniqueSurgeryID, 
        dbo.ExamCondition.ExamConditionID AS UniqueCompID, 
        dbo.ExamCondition.ConditionID AS CompTypeID, dbo.ExamTreatment.TreatmentID AS SurgeryTypeID, 
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
        (dbo.refCondition.Condition IN ('Dehiscence, dental', 'Ranula', 'Incision complications', 'Surgical complication')) 
        AND (dbo.ExamTreatment.Medication IN (N'Dental Dehiscence Repair', N'Dental Extraction', 
        N'Dental COHAT (Lv 1-3)', N'Dental COHAT (Lv 4-5)', N'Dental Extraction, Difficult', 'COHAT'))
        AND dbo.ExamTreatment.StatusDateTime BETWEEN '{reference_date}' AND '{last_date}'
        AND dbo.ExamCondition.DateTimeDiagnosed BETWEEN '{reference_date}' AND '{last_date}'
        AND (DATEDIFF(d, dbo.ExamTreatment.DateCreated, dbo.ExamCondition.DateCreated) >= - 2)
    """
    df2 = fetch_query(query)
    #Remove Off Site Animals
    df2=df2[df2['Location']!='Off Site Clinic']
    #Remove Dehiscense so we associate compliacations with COHAT
    df2=df2[df2['SurgeryType']!='Dental Dehiscence Repair']
    #Sort animals, keep first record and remove duplicates
    df2=df2.sort_values(['AnimalID','UniqueSurgeryID', 'UniqueCompID']).reset_index(drop=True)
    df2=df2.drop_duplicates(subset=['AnimalID','UniqueSurgeryID', 'UniqueCompID'])
    #Remove Duplicates
    df2['SurgeryDate']=pd.to_datetime(df2['SurgeryDate']).dt.date
    df2=df2.drop_duplicates(subset=['AnimalID','SurgeryType', 'SxComp'])
    df2=df2.sort_values(['AnimalID', 'SurgeryType','SxComp','SurgeryDate', 'CompDate'])
    # checks if delta of date is less than trigger
    trigger = pd.Timedelta(1, unit='d') # one day
    df2['flag4'] = np.where(df2['SurgeryDate']-df2['SurgeryDate'].shift() > trigger , 0, 1)
    trigger2 = pd.Timedelta(3, unit='d') # three day
    df2['flag5'] = np.where(df2['CompDate']-df2['CompDate'].shift() > trigger2 , 0, 1)
    # checks if all conditions above have been met
    df2['flag6'] = df2['flag4'] + df2['flag5']
    df2['flag7'] = np.where(df2['flag6'] ==2,1,0)
    #cleaning up
    numerator = df2[df2['flag7']==0]
    numerator = numerator.drop(['flag4', 'flag5', 'flag6', 'flag7', 'CompTypeID','SurgeryTypeID' ], axis=1)
    #get the complication that is the min days after surgery
    numerator_refined=numerator.groupby(['AnimalID', 'SxComp']).agg({'DaysAfterSurgery': pd.Series.min}).reset_index()
    numerator_refined=pd.merge(numerator_refined, numerator, on=['AnimalID', 'SxComp', 'DaysAfterSurgery'], how='inner')
    numerator_refined['SurgeryDate']=pd.to_datetime(numerator_refined['SurgeryDate']).dt.date
    numerator_refined['CompDate']=pd.to_datetime(numerator_refined['CompDate']).dt.date
    #create the multiple category for if there were multiple surgeries for a given complicaiton
    multiple=numerator_refined.groupby(['AnimalID', 'SxComp', 'SurgeryDate']).agg({'SurgeryID': np.size}).rename(columns={'SurgeryID': 'num_surg_mapping'}).reset_index()
    multiple=multiple[multiple['num_surg_mapping']>1]
    numerator_refined=pd.merge(numerator_refined, multiple, on=["AnimalID", 'SxComp', 'SurgeryDate'], how='outer', indicator=True)
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
    return





def run_dental_report(report_year):
    """Function to run all scripts"""
    numerator=combined_df(numerator_extraction, report_year-1,report_year)
    denominator=combined_df(denominator_extraction, report_year-1,report_year)
    create_dashboard_data(numerator, denominator, dashboard_data_path)
    save_to_excel(numerator, denominator, report_path)
    update_dashboard(dashboard_path)

    return



#Path to report on local server
report_filename="dental_complication.xlsx"
report_path=f"{config.SERVER_PATH}/sxcomp/{report_filename}"
#dashboard_path
dashboard_filename="SurgicalComplicationsDashboard.xlsx"
dashboard_path=f"{config.SERVER_PATH}/sxcomp/{dashboard_filename}"
dashboard_data_file="dental_complication_data.xlsx"
dashboard_data_path=f"{config.SERVER_PATH}/sxcomp/{dashboard_data_file}"