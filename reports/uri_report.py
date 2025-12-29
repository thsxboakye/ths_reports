import pandas as pd
from datetime import datetime
import os
from prefect import task
from dateutil.relativedelta import relativedelta
from database.ms_sql_connection import fetch_query
from utils.utils import save_to_excel, update_dashboard, combined_df
from environment.settings import config



def uri_numerator(year, month): 
    """Reads query and performs calculation based on year 
    and month values"""
    #Constructs date value for first of every month
    reference_date=f'{year}-{month:02}-01'
    query=f"""WITH numerator
    AS (SELECT DISTINCT
      Animal.AnimalID,
      Animal.Name,
      refSpecies.Species,
      AnimalDetails.DateOfBirth,
      txnVisit.IntakeType,
      txnVisit.IntakeSubType,
      txnVisit.tin_DateCreated AS IntakeDate,
      refCondition.Condition,
      ExamCondition.ExamID,
      ExamCondition.DateCreated AS ExamDate,
      DATEDIFF(DAY, txnVisit.tin_DateCreated, ExamCondition.DateCreated) AS DaysAfterIntake
    FROM Animal
    INNER JOIN refSpecies
      ON Animal.SpeciesID = refSpecies.SpeciesID
    INNER JOIN AnimalDetails
      ON Animal.AnimalID = AnimalDetails.AnimalID
    INNER JOIN ExamCondition
    INNER JOIN txnVisit
      ON ExamCondition.DateCreated > txnVisit.tin_DateCreated
      ON Animal.AnimalID = txnVisit.AnimalID
      AND Animal.AnimalID = ExamCondition.AnimalID
    INNER JOIN refCondition
      ON ExamCondition.ConditionID = refCondition.ConditionID
    WHERE (refSpecies.Species IN ('Cat', 'Dog'))
    AND (txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender'))
    AND (refCondition.Condition IN ('Kennel cough','URI, feline'))
    --URI only considered as 'occured within shelter' if occured after 3 days in shelter
    AND DATEDIFF(DAY, txnVisit.tin_DateCreated, ExamCondition.DateCreated) BETWEEN 4 AND 365
    --Checks for examdates in current month only
    AND ExamCondition.DateCreated >=  '{reference_date}' and 
    ExamCondition.DateCreated <= EOMONTH('{reference_date}')
    )
    SELECT
      numerator.animalid,
      name,
      species,
      dateofbirth,
      MAX(intaketype) as Intaketype,
      condition,
      MAX(intakedate) AS intakedate,
      MIN(examdate) AS examdate,
      CAST('{reference_date}' AS date) AS Referencedate,
      CASE
        WHEN DATEDIFF(WEEK, numerator.dateofbirth, '{reference_date}') >= 20 THEN 'Adult'
        WHEN DATEDIFF(WEEK, numerator.dateofbirth, '{reference_date}') < 20 AND
          numerator.species = 'Cat' THEN 'Kitten'
        ELSE 'Puppy'
      END AS Agegroup
    FROM numerator
    WHERE DateOfBirth IS NOT NULL 
    

    GROUP BY numerator.animalid,
             name,
             species,
             condition,
             dateofbirth"""
    
    return fetch_query(query)


def uri_denominator(year, month): 
    """Reads query and performs calculation based on year 
    and monthvalues"""
    #Constructs date value for first of every month
    reference_date=f'{year}-{month:02}-01'
    query= f"""

    /*The Denominator dataset compiles information on all pets present in the shelter at the beginning of the month, 
    as well as pets that arrived during that month. 
    It merges data from both inventory records and intake records using a UNION operation.*/

    /*Beginning of CTEs for inventory table cleaning */
    --Inventory table contains history records of all animals present in the shelter.
    WITH inventory_table
    AS (SELECT DISTINCT
      Animal.AnimalID,
      Animal.Name,
      refSpecies.Species,
      AnimalDetails.DateOfBirth,
      refAnimalStage.Stage,
      HistoryStatus.LastUpdated AS StageDate,
      txnVisit.IntakeType,
      txnVisit.IntakeSubType,
      txnVisit.tin_DateCreated AS intakedate,
      HistoryStatus.Status
    FROM HistoryStatus
    INNER JOIN Animal
      ON HistoryStatus.AnimalID = Animal.AnimalID
    INNER JOIN refSpecies
      ON Animal.SpeciesID = refSpecies.SpeciesID
    INNER JOIN AnimalDetails
      ON Animal.AnimalID = AnimalDetails.AnimalID
    LEFT OUTER JOIN txnVisit
      ON txnVisit.AnimalID = HistoryStatus.AnimalID
      AND txnVisit.InPrimaryKey = HistoryStatus.OperationPrimaryID
    LEFT OUTER JOIN refAnimalStage
      ON HistoryStatus.StageID = refAnimalStage.StageID
    LEFT OUTER JOIN Stray
      ON txnVisit.IntakeSubTypeID = Stray.IntakeSubTypeID
      AND txnVisit.AnimalID = Stray.AnimalID
    LEFT OUTER JOIN TransferIn
      ON txnVisit.IntakeSubTypeID = TransferIn.IntakeSubTypeID
      AND txnVisit.AnimalID = TransferIn.AnimalID
    LEFT OUTER JOIN OwnerSurrender
      ON txnVisit.IntakeSubTypeID = OwnerSurrender.IntakeSubTypeID
      AND txnVisit.AnimalID = OwnerSurrender.AnimalID
    LEFT OUTER JOIN [Return]
      ON txnVisit.IntakeSubTypeID = [Return].IntakeSubTypeID
      AND txnVisit.AnimalID = [Return].AnimalID
    WHERE (refSpecies.Species IN ('Cat', 'Dog'))
    AND (refAnimalStage.Stage IN (N'Released', N'Pre-Euthanasia', N'Foster Program',
    N'Evaluate', N'Stray Holding - Feline',
    N'Stray Holding - Canine', N'Pre-Intake',
    N'Surgery Needed', N'Pending Behavior Assessment', N'Bite Quarantine', N'Medical Observation',
    N'Foster Needed', N'Medical Treatment', N'Behavior Observation'))
    AND (txnVisit.IntakeType IN ('TransferIn',
    'OwnerSurrender', '[Return]', 'Stray')
    OR txnVisit.IntakeType IS NULL)
    --Fetches data from one year ago.
    AND (txnVisit.tin_DateCreated >= DATEADD(YEAR, -1, '{reference_date}')
    OR txnVisit.tin_DateCreated IS NULL)
    AND (HistoryStatus.LastUpdated >= DATEADD(YEAR, -1, '{reference_date}'))),

    --latest_stage_date is a CTE of most recent stage dates for each pet between a year back and current report month
    latest_stagedate
    AS (SELECT
      animalid,
      name,
      MAX(stagedate) AS stagedate
    FROM inventory_table
    WHERE stagedate > DATEADD(YEAR, -1, '{reference_date}')
    AND StageDate <= '{reference_date}'
    GROUP BY animalid,
            name),

    --Past_intakes is a CTE of records in inventory table where intakedate is less than current month date
    Past_Intakes
    AS (SELECT
      animalid,
      MAX(intakedate) AS intakedate
    FROM inventory_table
    WHERE intakedate IS NOT NULL
    AND intakedate < '{reference_date}'
    GROUP BY animalid),
    /*End of CTEs for inventory table cleaning */


    /* Beginning of CTEs for intake records data cleaning*/
    --Total_Intake table generates records of all animals that came in during the month.
    Total_Intake
    AS (SELECT DISTINCT
      txnVisit.tin_DateCreated AS IntakeDate,
      Animal.AnimalID,
      Animal.Name,
      refSpecies.Species,
      AnimalDetails.DateOfBirth,
      txnVisit.IntakeType,
      txnVisit.IntakeSubType,
      refOperationStatus.OperationStatus
    FROM refSpecies
    INNER JOIN Animal
    INNER JOIN txnVisit
      ON Animal.AnimalID = txnVisit.AnimalID
      ON Animal.SpeciesID = refSpecies.SpeciesID
    INNER JOIN AnimalDetails
      ON AnimalDetails.AnimalID = Animal.AnimalID
    LEFT OUTER JOIN IntakeStatusHistory
    INNER JOIN refOperationStatus
      ON IntakeStatusHistory.StatusID = refOperationStatus.OperationStatusID
      ON txnVisit.tin_DateCreated = IntakeStatusHistory.StatusDateTime
      AND txnVisit.InPrimaryKey = IntakeStatusHistory.OperationRecordID
    LEFT OUTER JOIN refCondition
    INNER JOIN ExamCondition
      ON refCondition.ConditionID = ExamCondition.ConditionID
      ON txnVisit.AnimalID = ExamCondition.AnimalID
    WHERE (refSpecies.Species IN ('Cat', 'Dog'))
    AND (txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender'))
    AND (refOperationStatus.OperationStatus = 'Completed')
    AND txnVisit.tin_DateCreated >= '{reference_date}'
    AND txnVisit.tin_DateCreated < EOMONTH('{reference_date}')),

    /*Intake_exclusion generates records of all animals to be excluded from intake records
    Animals who came to the shelter and developed uri within 3 days of coming into the shelter need to be excluded*/
    intake_exclusion
    AS (SELECT
      Animal.AnimalID
    FROM Animal
    INNER JOIN refSpecies
      ON Animal.SpeciesID = refSpecies.SpeciesID
    INNER JOIN AnimalDetails
      ON Animal.AnimalID = AnimalDetails.AnimalID
    INNER JOIN ExamCondition
    INNER JOIN txnVisit
      ON ExamCondition.DateCreated > txnVisit.tin_DateCreated
      ON Animal.AnimalID = txnVisit.AnimalID
      AND Animal.AnimalID = ExamCondition.AnimalID
    INNER JOIN refCondition
      ON ExamCondition.ConditionID = refCondition.ConditionID
    WHERE (refSpecies.Species IN ('Cat', 'Dog'))
    AND (txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender'))
    AND (refCondition.Condition IN ('Kennel cough','URI, feline'))
    --excludes animals that developed uri within 3 days
    AND DATEDIFF(DAY, txnVisit.tin_DateCreated, ExamCondition.DateCreated) BETWEEN 0 AND 3
    AND ExamCondition.DateCreated >= '{reference_date}'
    AND ExamCondition.DateCreated <= EOMONTH('{reference_date}'))
    /* End of CTEs for intake records data cleaning*/


    /*Beginning of data cleaning for inventory dataset*/

    --Obtains corresponding status and intakedate for most recent stagedate from latest_stagedate table
    --max(inventory_table.status) was used to obtain only one entry in situations where there was more than one status for same date.
    --Thus it assigns the maximum status(I=Inactive) of such occurences since such pets are to be ignored.
    --Min(inventory_table.stage) selects the first stage entry in cases where there are 2 stages for the same stagedate/status

    SELECT
      latest_stagedate.animalid,
      latest_stagedate.name,
      inventory_table.species,
      inventory_table.dateofbirth,
      latest_stagedate.stagedate,
      MAX(inventory_table.status) AS status,
      MIN(inventory_table.stage) AS stage,
      Past_Intakes.intakedate,
      CAST('{reference_date}' AS date) AS Referencedate,
      CASE
        WHEN DATEDIFF(WEEK, inventory_table.dateofbirth, '{reference_date}') >= 20 THEN 'Adult'
        WHEN DATEDIFF(WEEK, inventory_table.dateofbirth, '{reference_date}') < 20 AND
          inventory_table.species = 'Cat' THEN 'Kitten'
        ELSE 'Puppy'
      END AS Agegroup
    FROM latest_stagedate
    INNER JOIN inventory_table
      ON latest_stagedate.stagedate = inventory_table.stagedate
      AND latest_stagedate.animalid = inventory_table.animalid
    INNER JOIN Past_Intakes
      ON latest_stagedate.animalid = Past_Intakes.animalid
    WHERE inventory_table.status = 'A'
    AND dateofbirth IS NOT NULL

    GROUP BY latest_stagedate.animalid,
            inventory_table.species,
            latest_stagedate.stagedate,
            Past_Intakes.intakedate,
            latest_stagedate.name,
            inventory_table.dateofbirth
    /*End of data cleaning for inventory dataset*/

    UNION ALL

    /*Beginning of data cleaning for intake dataset.
    Generates dataset of all pets who came into the shelter during the month and excludes pets
    who generated uri within 3 days of coming into the shelter.*/

    SELECT
      total_intake.animalid,
      total_intake.name,
      total_intake.species,
      total_intake.dateofbirth,
      CAST('{reference_date}' AS date) AS Stagedate,
      'A' AS status,
      'Intake' AS stage,
      MAX(Total_Intake.intakedate),
      CAST('{reference_date}' AS date) AS Referencedate,
      CASE
        WHEN DATEDIFF(WEEK, total_intake.dateofbirth, '{reference_date}') >= 20 THEN 'Adult'
        WHEN DATEDIFF(WEEK, total_intake.dateofbirth, '{reference_date}') < 20 AND
          total_intake.species = 'Cat' THEN 'Kitten'
        ELSE 'Puppy'
      END AS Agegroup
    FROM Total_Intake
    --Excludes pets who developed uri within 3 days of coming into shelter
    WHERE NOT EXISTS (SELECT
      *
    FROM intake_exclusion
    WHERE intake_exclusion.AnimalID = Total_Intake.animalid)

    AND dateofbirth IS NOT NULL
    GROUP BY total_intake.animalid,
            total_intake.name,
            total_intake.species,
            total_intake.dateofbirth
    ORDER BY animalid
    """

    
    return fetch_query(query)


def parse_combined_data(function,report_year) -> pd.DataFrame:
    """Combines the dataframes for each year and months into a single dataframe"""
    
    df=combined_df(function, report_year, report_year)
    try:
      df[["dateofbirth", "intakedate", "Referencedate", "examdate"]] = df[["dateofbirth", "intakedate", "Referencedate", "examdate"]].apply(
          lambda x: pd.to_datetime(x).dt.date)
    except:
      df[["dateofbirth", "intakedate", "Referencedate", "stagedate"]] = df[["dateofbirth", "intakedate", "Referencedate", "stagedate"]].apply(
        lambda x: pd.to_datetime(x).dt.date)
    return df


def uri_chart(*,numerator, denominator, path) -> None:
  '''Creates harmonized records of numerators and denominators.
  This data is responsible for the dashboard building.'''
  #Creates an Outcome column specifying all numerator as infected.

  numerator["Outcome"]="Infected"
  #Assigns a denominator string to all intake types of denominator.
  #Since we need to pull intaketypes from numerator for dashboard.
  #Creates an Outcome column specifying all numerator as Healthy.
  denominator["Outcome"]="Healthy" 
  denominator["Intaketype"]="Denominator"	
  chart_data=pd.concat(
    [denominator[['animalid', 'species', 'Agegroup','Outcome','Intaketype', 'Referencedate']], 
    numerator[['animalid', 'species', 'Agegroup','Outcome','Intaketype', 'Referencedate']]],
    ignore_index=True
    )
  #Assign value of 1 to real data values
  #We will assign value of 0 to virtual data values used to account for months with no incidence data
  chart_data['sum']=1
  # Creating a Referencedate range with the missing months
  min_Referencedate = chart_data['Referencedate'].min()
  max_Referencedate = chart_data['Referencedate'].max()
  Referencedate_range = pd.date_range(min_Referencedate, max_Referencedate, freq='MS').date

  # Generating the missing rows for dogs
  missing_rows = []
  for agegroup in ['Adult', 'Puppy']:
    existing_Referencedates = chart_data[(chart_data['Outcome'] == 'Infected') & (chart_data['species'] == 'Dog') & (chart_data['Agegroup'] == agegroup)]['Referencedate']
    missing_Referencedates = set(Referencedate_range) - set(existing_Referencedates)
    for Referencedate in missing_Referencedates:
        missing_rows.append({'Referencedate': Referencedate, 'Agegroup': agegroup, 'species': 'Dog', 'Outcome': 'Infected', 'sum':0})
   # Generating the missing rows for cats
  for agegroup in ['Adult', 'Kitten']:
    existing_Referencedates = chart_data[(chart_data['Outcome'] == 'Infected') & (chart_data['species'] == 'Cat') & (chart_data['Agegroup'] == agegroup)]['Referencedate']
    missing_Referencedates = set(Referencedate_range) - set(existing_Referencedates)
    for Referencedate in missing_Referencedates:
        missing_rows.append({'Referencedate': Referencedate, 'Agegroup': agegroup, 'species': 'Cat', 'Outcome': 'Infected', 'sum':0})
  
  # Convert missing rows to DataFrame before concatenating
  missing_df = pd.DataFrame(missing_rows)
  # Appending the missing rows to the DataFrame
  chart_data=pd.concat([chart_data, missing_df], ignore_index=True)
  chart_data.to_excel(path, index=False)
  return chart_data

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
    df['Referencedate'] = pd.to_datetime(df['Referencedate'])

    # Get today's date normalized to midnight
    today = pd.Timestamp.today().normalize()

    # First day of the current month
    max_date = today.replace(day=1)

    # Start date = first day of the month 12 months ago
    start_date = max_date - relativedelta(months=13)

    # Filter SurgeryDate between start_date (inclusive) and max_date (exclusive)
    mask = (df['Referencedate'] >= start_date) & (df['Referencedate'] < max_date)
    filtered_df = df.loc[mask]

    return filtered_df

def process_incidence_bi_data(df):
    df['Referencedate'] = pd.to_datetime(df['Referencedate'])
    df['Month'] = df['Referencedate'].dt.to_period('M')
    grouped = df.groupby(['species', 'Month', 'Agegroup', 'Outcome'])['sum'].sum().reset_index()
    # Get current month as Period (e.g., '2025-05')
    current_month = pd.Period(datetime.today(), freq='M')
    # Remove records from the current month
    df = df[df['Month'] != current_month]
    
    pivot_df = grouped.pivot_table(
        index=['species', 'Month', 'Agegroup'],
        columns='Outcome',
        values='sum',
        fill_value=0
    ).reset_index()
    
    pivot_df['infection_percent'] = pivot_df['Infected'] / pivot_df['Healthy']
    pivot_df['infection_percent'] = (pivot_df['infection_percent'] * 100).round(2)
    
    return pivot_df[['species', 'Month', 'Agegroup', 'infection_percent']]



def run_uri_report(report_year):
  df_denom=parse_combined_data(uri_denominator, report_year)
  df_num=parse_combined_data(uri_numerator, report_year)
 # Parse previous year data
  print("Fetching denominator data for previous year...")
  df_denom_prev_year = parse_combined_data(uri_denominator, report_year-1)
  print(f"Denominator data for {report_year-1} loaded. Shape: {df_denom_prev_year.shape}")

  print("Fetching numerator data for previous year...")
  df_num_prev_year = parse_combined_data(uri_numerator, report_year-1)
  print(f"Numerator data for {report_year-1} loaded. Shape: {df_num_prev_year.shape}")
  two_year_denom=pd.concat([df_denom_prev_year,df_denom], ignore_index=True)
  two_year_num=pd.concat([df_num_prev_year,df_num], ignore_index=True)
  bi_data=uri_chart(path=bi_report_path, numerator=two_year_num, denominator=two_year_denom)
  bi_data=filter_last_12_months(bi_data)
  bi_data=process_incidence_bi_data(bi_data)
  bi_data.to_excel(bi_report_path,index=False)
  
  save_to_excel(path=report_path,numerator=df_num, denominator=df_denom)
  uri_chart(path=chart_path, numerator=df_num, denominator=df_denom)
  update_dashboard(dashboard_path)
 


#Path to power_bi_data report on local server
bi_report_filename="uri_infection_percent_bi_data.xlsx"
bi_report_path=f"{config.SERVER_PATH}/power_bi/{bi_report_filename}"
#Path to report on local server
report_filename="uri_report.xlsx"
report_path=f"{config.SERVER_PATH}/uri/{report_filename}"
#chart data path
chart_filename="uri_chart_data.xlsx"
chart_path=f"{config.SERVER_PATH}/uri/{chart_filename}"
#dashboard_path
dashboard_filename="uri_report_dashBoard.xlsx"
dashboard_path=f"{config.SERVER_PATH}/uri/{dashboard_filename}"
