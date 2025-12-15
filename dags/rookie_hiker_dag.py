from airflow.decorators import dag, task
from datetime import datetime
import os
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# --- Data Paths ---
RAW_DATA_PATH = '/opt/airflow/data/raw'
GOLD_DATA_PATH = '/opt/airflow/data/gold'
NBA_STATS_FILE = os.path.join(RAW_DATA_PATH, 'NBA_stats_data.csv')
TRAILS_FILE = os.path.join(RAW_DATA_PATH, 'HikingTrails_TheGorge.csv')
HAZARDS_FILE = os.path.join(RAW_DATA_PATH, 'Trail_hazards_danger.csv')
REPORT_FILE = os.path.join(GOLD_DATA_PATH, 'top_10_rookie_hikers.png')

# --- 2022-2023 Rookie List ---
# Since the dataset doesn't have a "Rookie" flag, we filter by known names from this season.
ROOKIE_LIST = [
    'Paolo Banchero', 'Jalen Williams', 'Walker Kessler', 'Keegan Murray', 
    'Bennedict Mathurin', 'Jaden Ivey', 'Jabari Smith Jr.', 'Jeremy Sochan', 
    'Jalen Duren', 'Tari Eason', 'Shaedon Sharpe', 'AJ Griffin', 
    'Christian Braun', 'Jaylin Williams', 'Andrew Nembhard', 'Walker Kessler'
]

# --- Helper Functions ---
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

    @task
    def compute_rookie_compatibility():
        # 1. Load Data
        players_df = pd.read_csv(NBA_STATS_FILE)
        trails_df = pd.read_csv(TRAILS_FILE)
        hazards_df = pd.read_csv(HAZARDS_FILE)

        # 2. Filter for Rookies Only
        # We strip whitespace to ensure matching works
        players_df['NAME'] = players_df['NAME'].astype(str).str.strip()
        players_df = players_df[players_df['NAME'].isin(ROOKIE_LIST)].copy()

        if players_df.empty:
            print("No rookies found in the dataset matching the list.")
            return []

        # 3. Clean Stats
        stat_cols = ['GP','MPG','RPG','APG','SPG','BPG']
        for col in stat_cols:
            if col in players_df.columns:
                players_df[col] = pd.to_numeric(players_df[col], errors='coerce').fillna(0)

        # 4. Calculate Player Scores
        players_df['endurance_score'] = players_df['MPG'] * players_df['GP']
        players_df['strength_score'] = players_df['RPG'] + players_df['BPG']
        players_df['agility_score'] = players_df['SPG'] + players_df['APG']

        # 5. Clean Trails & Hazards
        trails_df['distance_miles'] = trails_df['Distance'].apply(get_distance)
        trails_df['elevation_ft'] = trails_df['Elevation Gain'].apply(get_elevation)

        hazards_df['has_falling_risk'] = (
            hazards_df['Falling'].astype(str).str.strip().str.lower()
            .map({'yes':1,'y':1,'true':1,'1':1}).fillna(0).astype(int)
        )
        
        trails_df = trails_df.merge(
            hazards_df[['Name','has_falling_risk']], 
            left_on='Trail Name', right_on='Name', how='left'
        )

        # 6. Calculate Trail Requirements
        trails_df['endurance_req'] = (trails_df['distance_miles'] * 50) + (trails_df['elevation_ft'] * 0.1)
        trails_df['agility_req'] = np.where(trails_df['has_falling_risk'] > 0, 5, 0)
        trails_df['difficulty_norm'] = trails_df['Difficulty'].astype(str).str.strip().str.lower()

        # 7. Cross Join (Cartesian Product)
        players_df['_k'] = 1
        trails_df['_k'] = 1
        pairs = players_df.merge(trails_df, on='_k').drop(columns=['_k'])

        # 8. Determine Compatibility
        cond_base = (pairs['endurance_score'] > pairs['endurance_req']) & (pairs['agility_score'] > pairs['agility_req'])
        
        diff = pairs['difficulty_norm']
        cond_diff = np.where(
            diff.eq('difficult'), pairs['strength_score'] > 10,
            np.where(diff.eq('moderate'), pairs['strength_score'] > 5, True)
        )

        compatible = pairs[cond_base & cond_diff]

        # 9. Aggregate Top 10 Rookies
        top_rookies = (compatible.groupby('NAME')['Trail Name']
                       .nunique()
                       .reset_index(name='compatible_trails')
                       .sort_values('compatible_trails', ascending=False)
                       .head(10))

        return top_rookies.to_dict('records')

    @task
    def generate_rookie_plot(rookie_data):
        if not rookie_data:
            print("No data to plot.")
            return

        df = pd.DataFrame(rookie_data)
        
        # Sort ascending for horizontal bar chart
        df = df.sort_values('compatible_trails', ascending=True)

        plt.figure(figsize=(10, 6))
        plt.barh(df['NAME'], df['compatible_trails'], color='skyblue')
        plt.xlabel('Number of Compatible Trails')
        plt.title('Top 10 NBA Rookies by Hiking Compatibility')
        plt.tight_layout()

        os.makedirs(GOLD_DATA_PATH, exist_ok=True)
        plt.savefig(REPORT_FILE)
        print(f"Rookie plot saved to {REPORT_FILE}")

    # DAG Flow
    data = compute_rookie_compatibility()
    generate_rookie_plot(data)

rookie_hiker_pipeline()