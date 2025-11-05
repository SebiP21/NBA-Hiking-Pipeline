from airflow.decorators import dag, task
from datetime import datetime
import os
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# --- Data Paths (inside container) ---
RAW_DATA_PATH = '/opt/airflow/data/raw'
GOLD_DATA_PATH = '/opt/airflow/data/gold'
NBA_STATS_FILE = os.path.join(RAW_DATA_PATH, 'NBA_stats_data.csv')
TRAILS_FILE = os.path.join(RAW_DATA_PATH, 'HikingTrails_TheGorge.csv')
HAZARDS_FILE = os.path.join(RAW_DATA_PATH, 'Trail_hazards_danger.csv')
REPORT_FILE = os.path.join(GOLD_DATA_PATH, 'player_trail_report.png')

# --- Helper Functions for cleaning ---
def get_distance(text):
    match = re.search(r'(\d+\.?\d*)', str(text))
    return float(match.group(1)) if match else 0.0

def get_elevation(text):
    match = re.search(r'([\d,]+)', str(text))
    return float(match.group(1).replace(',', '')) if match else 0.0

@dag(
    dag_id='nba_hiking_compatibility_pipeline_pandas_only',
    start_date=datetime(2023, 1, 1),
    schedule=None,   # Airflow 2.6+ (instead of schedule_interval=None)
    catchup=False,
    tags=['nba', 'hiking', 'pandas', 'matplotlib', 'no-db'],
)
def nba_hiking_elt_pipeline():
    """
    ELT pipeline (pandas-only):
    1) Extract & Transform: read CSVs, compute player scores & trail requirements.
    2) Compute: determine player↔trail compatibility purely in pandas.
    3) Report: save a bar chart of top 15 players by compatible trails.
    """

    @task
    def extract_and_transform():
        # Validate input files exist
        for p in [NBA_STATS_FILE, TRAILS_FILE, HAZARDS_FILE]:
            if not os.path.exists(p):
                raise FileNotFoundError(f"Missing required input file: {p}")

        # Read CSVs
        players_df = pd.read_csv(NBA_STATS_FILE)
        trails_df  = pd.read_csv(TRAILS_FILE)
        hazards_df = pd.read_csv(HAZARDS_FILE)

        # --- Clean Players ---
        stat_cols = [c for c in ['AGE','GP','MPG','RPG','APG','SPG','BPG'] if c in players_df.columns]
        for col in stat_cols:
            players_df[col] = pd.to_numeric(players_df[col], errors='coerce').fillna(0)

        players_df['endurance_score'] = players_df.get('MPG', 0) * players_df.get('GP', 0)
        players_df['strength_score']  = players_df.get('RPG', 0) + players_df.get('BPG', 0)
        players_df['agility_score']   = players_df.get('SPG', 0) + players_df.get('APG', 0)

        players_df['NAME'] = players_df['NAME'].astype(str).str.strip()
        players_df = players_df[players_df['NAME'].ne('')]

        # --- Clean Trails + Hazards ---
        trails_df['distance_miles'] = trails_df['Distance'].apply(get_distance)
        trails_df['elevation_ft']   = trails_df['Elevation Gain'].apply(get_elevation)

        # Robust hazard flag mapping
        hazards_df['has_falling_risk'] = (
            hazards_df['Falling'].astype(str).str.strip().str.lower()
            .map({'yes':1,'y':1,'true':1,'1':1}).fillna(0).astype(int)
        )

        trails_df = trails_df.merge(
            hazards_df[['Name','has_falling_risk']],
            left_on='Trail Name', right_on='Name', how='left'
        ).drop(columns=['Name'])

        # Requirements
        trails_df['endurance_req'] = (trails_df['distance_miles'] * 50) + (trails_df['elevation_ft'] * 0.1)
        trails_df['agility_req']   = np.where(trails_df['has_falling_risk'] > 0, 5, 0)

        # Normalize difficulty for comparison
        trails_df['difficulty_norm'] = trails_df['Difficulty'].astype(str).str.strip().str.lower()

        # Keep compact projections for XCom
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

        # Cartesian join (OK for small/medium data)
        players['_k'] = 1
        trails['_k']  = 1
        pairs = players.merge(trails, on='_k').drop(columns=['_k'])

        # Base compatibility
        cond_base = (pairs['endurance_score'] > pairs['endurance_req']) & (pairs['agility_score'] > pairs['agility_req'])

        # Difficulty thresholds
        diff = pairs['difficulty_norm']
        cond_diff = np.where(
            diff.eq('difficult'), pairs['strength_score'] > 10,
            np.where(diff.eq('moderate'), pairs['strength_score'] > 5, True)
        )

        compatible = pairs[cond_base & cond_diff]

        # Aggregate trails per player
        summary = (compatible.groupby('NAME')['Trail Name']
                   .nunique()
                   .reset_index(name='compatible_trails_count')
                   .sort_values('compatible_trails_count', ascending=False))

        top15 = summary.head(15)
        return top15.to_dict('records')

    @task
    def save_plot(top_records):
        result_df = pd.DataFrame(top_records)
        if result_df.empty:
            print("❌ No compatibility matches found. Skipping plot.")
            return

        os.makedirs(GOLD_DATA_PATH, exist_ok=True)
        plt.figure(figsize=(12, 8))
        plt.barh(result_df['NAME'], result_df['compatible_trails_count'])
        plt.xlabel('Number of Compatible Trails')
        plt.ylabel('NBA Player')
        plt.title('Top 15 NBA Players by Hiking Trail Compatibility')
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.savefig(REPORT_FILE)
        print(f"✅ Report plot saved to {REPORT_FILE}")

    data = extract_and_transform()
    top  = compute_compatibility(data)
    save_plot(top)

nba_hiking_elt_pipeline()
