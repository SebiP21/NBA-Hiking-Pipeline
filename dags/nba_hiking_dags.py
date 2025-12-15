from airflow.decorators import dag, task
from datetime import datetime
import os
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# defining the paths where our data lives inside the airflow container
RAW_DATA_PATH = '/opt/airflow/data/raw'
GOLD_DATA_PATH = '/opt/airflow/data/gold'
NBA_STATS_FILE = os.path.join(RAW_DATA_PATH, 'NBA_stats_data.csv')
TRAILS_FILE = os.path.join(RAW_DATA_PATH, 'HikingTrails_TheGorge.csv')
HAZARDS_FILE = os.path.join(RAW_DATA_PATH, 'Trail_hazards_danger.csv')
REPORT_FILE = os.path.join(GOLD_DATA_PATH, 'player_trail_report.png')

# helper function to extract distance as a float from the text field
def get_distance(text):
    match = re.search(r'(\d+\.?\d*)', str(text))
    return float(match.group(1)) if match else 0.0

# helper function to parse elevation, removing commas from numbers
def get_elevation(text):
    match = re.search(r'([\d,]+)', str(text))
    return float(match.group(1).replace(',', '')) if match else 0.0

@dag(
    dag_id='nba_hiking_compatibility_pipeline_pandas_only',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['nba', 'hiking', 'pandas', 'matplotlib', 'no-db'],
    template_searchpath=['/opt/airflow/sql']
)
def nba_hiking_elt_pipeline():
    """
    ELT pipeline overview:
    1. Initialize the Postgres tables using the SQL scripts I wrote.
    2. Populate those tables with raw data from the CSVs so we can see it in pgAdmin.
    3. Use Pandas to clean up the stats and calculate the 'hiking scores'.
    4. figure out which players fit which trails.
    5. finally, generate a chart for the top 15 players.
    """

    @task
    def load_raw_csvs_to_postgres():
        """
        Takes the raw CSV files and pushes them into Postgres.
        This is mostly so we can verify the data exists in the DB tool (pgAdmin).
        """
        # mapping the csv file paths to the table names created by the sql operators
        files_to_tables = {
            NBA_STATS_FILE: 'nba_stats_data_raw',
            TRAILS_FILE: 'hiking_trails_thegorge_raw',
            HAZARDS_FILE: 'trail_hazards_danger_raw'
        }

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = pg_hook.get_sqlalchemy_engine()

        for file_path, table_name in files_to_tables.items():
            if os.path.exists(file_path):
                print(f"Starting load for {file_path} into {table_name}...")
                df = pd.read_csv(file_path)
                
                # using replace here to make sure we don't duplicate data if we re-run this
                # keeping index=False because we don't need the pandas index in the db
                df.to_sql(table_name, con=engine, if_exists='replace', index=False)
                print(f"Finished loading {len(df)} rows into {table_name}.")
            else:
                print(f"Warning: Could not find file {file_path}.")

    @task
    def extract_and_transform():
        # quick check to make sure all the necessary files are actually there
        for p in [NBA_STATS_FILE, TRAILS_FILE, HAZARDS_FILE]:
            if not os.path.exists(p):
                raise FileNotFoundError(f"Missing required input file: {p}")

        # reading the raw data into dataframes
        players_df = pd.read_csv(NBA_STATS_FILE)
        trails_df  = pd.read_csv(TRAILS_FILE)
        hazards_df = pd.read_csv(HAZARDS_FILE)

        # need to clean up the numeric columns in the player stats
        # sometimes these come in as objects/strings, so coercing them to numbers
        stat_cols = [c for c in ['AGE','GP','MPG','RPG','APG','SPG','BPG'] if c in players_df.columns]
        for col in stat_cols:
            players_df[col] = pd.to_numeric(players_df[col], errors='coerce').fillna(0)

        # calculating the hiking attributes based on basketball stats
        # endurance is minutes played * games played
        players_df['endurance_score'] = players_df.get('MPG', 0) * players_df.get('GP', 0)
        # strength combines rebounds and blocks
        players_df['strength_score']  = players_df.get('RPG', 0) + players_df.get('BPG', 0)
        # agility is steals plus assists
        players_df['agility_score']   = players_df.get('SPG', 0) + players_df.get('APG', 0)

        # cleaning up player names to avoid matching issues later
        players_df['NAME'] = players_df['NAME'].astype(str).str.strip()
        players_df = players_df[players_df['NAME'].ne('')]

        # parsing the trail data numbers using the helper functions
        trails_df['distance_miles'] = trails_df['Distance'].apply(get_distance)
        trails_df['elevation_ft']   = trails_df['Elevation Gain'].apply(get_elevation)

        # normalizing the hazard data - converting 'yes'/'y'/'true' to 1 for easier math
        hazards_df['has_falling_risk'] = (
            hazards_df['Falling'].astype(str).str.strip().str.lower()
            .map({'yes':1,'y':1,'true':1,'1':1}).fillna(0).astype(int)
        )

        # joining hazards to trails so we know which trails are dangerous
        trails_df = trails_df.merge(
            hazards_df[['Name','has_falling_risk']],
            left_on='Trail Name', right_on='Name', how='left'
        ).drop(columns=['Name'])

        # calculating what the trails require from a hiker
        trails_df['endurance_req'] = (trails_df['distance_miles'] * 50) + (trails_df['elevation_ft'] * 0.1)
        # if there is a falling risk, we set a high agility requirement
        trails_df['agility_req']   = np.where(trails_df['has_falling_risk'] > 0, 5, 0)

        # normalizing the difficulty string so we can filter on it easily
        trails_df['difficulty_norm'] = trails_df['Difficulty'].astype(str).str.strip().str.lower()

        # keeping only the columns we actually need for the compatibility logic
        players = players_df[['NAME','endurance_score','strength_score','agility_score']].copy()
        trails  = trails_df[['Trail Name','endurance_req','agility_req','difficulty_norm']].copy()

        return {
            "players": players.to_dict('records'),
            "trails": trails.to_dict('records')
        }

    @task
    def compute_compatibility(data):
        players = pd.DataFrame(data['players'])
        trails  = pd.DataFrame(data['trails'])

        # doing a cross join here to compare every player against every trail
        # this creates a lot of rows but it's fine for this dataset size
        players['_k'] = 1
        trails['_k']  = 1
        pairs = players.merge(trails, on='_k').drop(columns=['_k'])

        # checking if player stats meet the trail requirements
        cond_base = (pairs['endurance_score'] > pairs['endurance_req']) & (pairs['agility_score'] > pairs['agility_req'])

        # checking difficulty: tough trails need more strength
        diff = pairs['difficulty_norm']
        cond_diff = np.where(
            diff.eq('difficult'), pairs['strength_score'] > 10,
            np.where(diff.eq('moderate'), pairs['strength_score'] > 5, True)
        )

        compatible = pairs[cond_base & cond_diff]

        # counting how many trails each player can hike
        summary = (compatible.groupby('NAME')['Trail Name']
                   .nunique()
                   .reset_index(name='compatible_trails_count')
                   .sort_values('compatible_trails_count', ascending=False))

        # just taking the top 15 for the report
        top15 = summary.head(15)
        return top15.to_dict('records')

    @task
    def save_plot(top_records):
        result_df = pd.DataFrame(top_records)
        if result_df.empty:
            print("No compatibility matches found. Skipping plot.")
            return

        # ensuring the output directory exists before saving
        os.makedirs(GOLD_DATA_PATH, exist_ok=True)
        
        # plotting the results
        plt.figure(figsize=(12, 8))
        plt.barh(result_df['NAME'], result_df['compatible_trails_count'])
        plt.xlabel('Number of Compatible Trails')
        plt.ylabel('NBA Player')
        plt.title('Top 15 NBA Players by Hiking Trail Compatibility')
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.savefig(REPORT_FILE)
        print(f"Report plot saved to {REPORT_FILE}")

    # creating the empty tables in postgres using the SQL files
    create_nba_stats_table = PostgresOperator(
        task_id='create_nba_stats_table',
        postgres_conn_id='postgres_default',
        sql='create_nba_stats_table.sql',
        autocommit=True,
    )

    create_hiking_trails_table = PostgresOperator(
        task_id='create_hiking_trails_table',
        postgres_conn_id='postgres_default',
        sql='create_hiking_trails_table.sql',
        autocommit=True,
    )

    create_trail_hazards_table = PostgresOperator(
        task_id='create_trail_hazards_table',
        postgres_conn_id='postgres_default',
        sql='create_trail_hazards_table.sql',
        autocommit=True,
    )

    # defining the execution order
    # 1. create schema -> 2. load raw csv data -> 3. process logic
    
    tables_created = [create_nba_stats_table, create_hiking_trails_table, create_trail_hazards_table]
    
    data_loaded = load_raw_csvs_to_postgres()
    
    processed_data = extract_and_transform()
    top_hikers = compute_compatibility(processed_data)
    
    tables_created >> data_loaded >> processed_data
    save_plot(top_hikers)

nba_hiking_elt_pipeline()