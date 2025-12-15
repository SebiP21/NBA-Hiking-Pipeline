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
REPORT_FILE = os.path.join(GOLD_DATA_PATH, 'top_5_hiking_teams.png')

# --- Helper Functions ---
def get_distance(text):
    match = re.search(r'(\d+\.?\d*)', str(text))
    return float(match.group(1)) if match else 0.0

def get_elevation(text):
    match = re.search(r'([\d,]+)', str(text))
    return float(match.group(1).replace(',', '')) if match else 0.0

@dag(
    dag_id='nba_teams_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['nba', 'hiking', 'teams']
)
def nba_teams_pipeline():

    @task
    def compute_team_compatibility():
        # 1. Load Data
        players_df = pd.read_csv(NBA_STATS_FILE)
        trails_df = pd.read_csv(TRAILS_FILE)
        hazards_df = pd.read_csv(HAZARDS_FILE)

        # 2. Clean Stats & Names
        players_df['NAME'] = players_df['NAME'].astype(str).str.strip()
        players_df['TEAM'] = players_df['TEAM'].astype(str).str.strip()
        # Filter out empty rows
        players_df = players_df[players_df['NAME'].ne('')]

        stat_cols = ['GP','MPG','RPG','APG','SPG','BPG']
        for col in stat_cols:
            if col in players_df.columns:
                players_df[col] = pd.to_numeric(players_df[col], errors='coerce').fillna(0)

        # 3. Calculate Player Scores
        players_df['endurance_score'] = players_df['MPG'] * players_df['GP']
        players_df['strength_score'] = players_df['RPG'] + players_df['BPG']
        players_df['agility_score'] = players_df['SPG'] + players_df['APG']

        # 4. Clean Trails & Hazards
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

        # 5. Calculate Trail Requirements
        trails_df['endurance_req'] = (trails_df['distance_miles'] * 50) + (trails_df['elevation_ft'] * 0.1)
        trails_df['agility_req'] = np.where(trails_df['has_falling_risk'] > 0, 5, 0)
        trails_df['difficulty_norm'] = trails_df['Difficulty'].astype(str).str.strip().str.lower()

        # 6. Cross Join (Cartesian Product)
        # We only keep necessary columns to optimize memory usage
        p_mini = players_df[['NAME', 'TEAM', 'endurance_score', 'strength_score', 'agility_score']].copy()
        t_mini = trails_df[['Trail Name', 'endurance_req', 'agility_req', 'difficulty_norm']].copy()

        p_mini['_k'] = 1
        t_mini['_k'] = 1
        pairs = p_mini.merge(t_mini, on='_k').drop(columns=['_k'])

        # 7. Determine Compatibility
        cond_base = (pairs['endurance_score'] > pairs['endurance_req']) & (pairs['agility_score'] > pairs['agility_req'])
        
        diff = pairs['difficulty_norm']
        cond_diff = np.where(
            diff.eq('difficult'), pairs['strength_score'] > 10,
            np.where(diff.eq('moderate'), pairs['strength_score'] > 5, True)
        )

        compatible = pairs[cond_base & cond_diff]

        # 8. Aggregate by Player first (count of trails per player)
        player_counts = (compatible.groupby(['NAME', 'TEAM'])['Trail Name']
                         .nunique()
                         .reset_index(name='player_trail_count'))

        # 9. Aggregate by Team (Average trails per player)
        team_stats = (player_counts.groupby('TEAM')['player_trail_count']
                      .mean()
                      .reset_index(name='avg_trails_per_player')
                      .sort_values('avg_trails_per_player', ascending=False)
                      .head(5))

        return team_stats.to_dict('records')

    @task
    def generate_team_plot(team_data):
        if not team_data:
            print("No team data to plot.")
            return

        df = pd.DataFrame(team_data)
        
        # Sort ascending for horizontal bar chart
        df = df.sort_values('avg_trails_per_player', ascending=True)

        plt.figure(figsize=(10, 6))
        plt.barh(df['TEAM'], df['avg_trails_per_player'], color='mediumseagreen')
        plt.xlabel('Average Compatible Trails per Player')
        plt.title('Top 5 NBA Teams by Hiking Compatibility')
        plt.tight_layout()

        os.makedirs(GOLD_DATA_PATH, exist_ok=True)
        plt.savefig(REPORT_FILE)
        print(f"Team report saved to {REPORT_FILE}")

    # DAG Flow
    data = compute_team_compatibility()
    generate_team_plot(data)

nba_teams_pipeline()