from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# defining paths again, same as the main dag
RAW_DATA_PATH = '/opt/airflow/data/raw'
GOLD_DATA_PATH = '/opt/airflow/data/gold'
NBA_STATS_FILE = os.path.join(RAW_DATA_PATH, 'NBA_stats_data.csv')
TRAILS_FILE = os.path.join(RAW_DATA_PATH, 'HikingTrails_TheGorge.csv')
HAZARDS_FILE = os.path.join(RAW_DATA_PATH, 'Trail_hazards_danger.csv')
REPORT_FILE = os.path.join(GOLD_DATA_PATH, 'top_10_rookie_hikers.png')

# manual list of 2022-2023 rookies since the dataset doesn't have a rookie flag column
ROOKIE_LIST = [
    'Paolo Banchero', 'Jalen Williams', 'Walker Kessler', 'Keegan Murray', 
    'Bennedict Mathurin', 'Jaden Ivey', 'Jabari Smith Jr.', 'Jeremy Sochan', 
    'Jalen Duren', 'Tari Eason', 'Shaedon Sharpe', 'AJ Griffin', 
    'Christian Braun', 'Jaylin Williams', 'Andrew Nembhard'
]

def get_distance(text):
    match = re.search(r'(\d+\.?\d*)', str(text))
    return float(match.group(1)) if match else 0.0

def get_elevation(text):
    match = re.search(r'([\d,]+)', str(text))
    return float(match.group(1).replace(',', '')) if match else 0.0

@dag(
    dag_id='rookie_hiker_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['nba', 'hiking', 'rookies']
)
def rookie_hiker_pipeline():

    # creating a specific table just for the rookie results
    # this way we can query the final results in pgAdmin later
    create_table = PostgresOperator(
        task_id='create_rookie_table',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS gold_top_10_rookies (
                "NAME" TEXT,
                "compatible_trails" INTEGER
            );
        """
    )

    @task
    def compute_and_load_rookies():
        # loading the raw csvs
        players_df = pd.read_csv(NBA_STATS_FILE)
        trails_df = pd.read_csv(TRAILS_FILE)
        hazards_df = pd.read_csv(HAZARDS_FILE)

        # cleaning player names and filtering only for the rookies in our list
        players_df['NAME'] = players_df['NAME'].astype(str).str.strip()
        players_df = players_df[players_df['NAME'].isin(ROOKIE_LIST)].copy()

        if players_df.empty:
            print("Found no rookies matching the list.")
            return []

        # converting stat columns to numeric, handling any errors
        stat_cols = ['GP','MPG','RPG','APG','SPG','BPG']
        for col in stat_cols:
            if col in players_df.columns:
                players_df[col] = pd.to_numeric(players_df[col], errors='coerce').fillna(0)

        # calculating physical attributes
        players_df['endurance_score'] = players_df['MPG'] * players_df['GP']
        players_df['strength_score'] = players_df['RPG'] + players_df['BPG']
        players_df['agility_score'] = players_df['SPG'] + players_df['APG']

        # parsing trail difficulty metrics
        trails_df['distance_miles'] = trails_df['Distance'].apply(get_distance)
        trails_df['elevation_ft'] = trails_df['Elevation Gain'].apply(get_elevation)

        # mapping 'Yes' strings to 1 so we can do math on falling risk
        hazards_df['has_falling_risk'] = (
            hazards_df['Falling'].astype(str).str.strip().str.lower()
            .map({'yes':1,'y':1,'true':1,'1':1}).fillna(0).astype(int)
        )
        
        # attaching hazard info to the trails
        trails_df = trails_df.merge(
            hazards_df[['Name','has_falling_risk']], 
            left_on='Trail Name', right_on='Name', how='left'
        )

        # defining what it takes to hike these trails
        trails_df['endurance_req'] = (trails_df['distance_miles'] * 50) + (trails_df['elevation_ft'] * 0.1)
        trails_df['agility_req'] = np.where(trails_df['has_falling_risk'] > 0, 5, 0)
        trails_df['difficulty_norm'] = trails_df['Difficulty'].astype(str).str.strip().str.lower()

        # preparing for the cross join
        players_df['_k'] = 1
        trails_df['_k'] = 1
        pairs = players_df.merge(trails_df, on='_k').drop(columns=['_k'])

        # determining who can hike what
        cond_base = (pairs['endurance_score'] > pairs['endurance_req']) & (pairs['agility_score'] > pairs['agility_req'])
        diff = pairs['difficulty_norm']
        cond_diff = np.where(
            diff.eq('difficult'), pairs['strength_score'] > 10,
            np.where(diff.eq('moderate'), pairs['strength_score'] > 5, True)
        )

        compatible = pairs[cond_base & cond_diff]

        # finding the top 10 rookies
        top_rookies = (compatible.groupby('NAME')['Trail Name']
                       .nunique()
                       .reset_index(name='compatible_trails')
                       .sort_values('compatible_trails', ascending=False)
                       .head(10))

        # writing the results back to postgres so we have a record in the db
        print("Loading results into Postgres table 'gold_top_10_rookies'...")
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = pg_hook.get_sqlalchemy_engine()
        
        # replacing table contents with fresh calculation
        top_rookies.to_sql('gold_top_10_rookies', con=engine, if_exists='replace', index=False)
        print("Data loaded successfully.")

        return top_rookies.to_dict('records')

    @task
    def generate_rookie_plot(rookie_data):
        if not rookie_data:
            return
        
        # plotting the data for the report
        df = pd.DataFrame(rookie_data).sort_values('compatible_trails', ascending=True)
        plt.figure(figsize=(10, 6))
        plt.barh(df['NAME'], df['compatible_trails'], color='skyblue')
        plt.xlabel('Number of Compatible Trails')
        plt.title('Top 10 NBA Rookies by Hiking Compatibility')
        plt.tight_layout()
        os.makedirs(GOLD_DATA_PATH, exist_ok=True)
        plt.savefig(REPORT_FILE)
        print(f"Plot saved to {REPORT_FILE}")

    # execution flow: create db table -> calculate stats & load db -> generate chart
    create_table >> compute_and_load_rookies() >> generate_rookie_plot(compute_and_load_rookies())

rookie_hiker_pipeline()