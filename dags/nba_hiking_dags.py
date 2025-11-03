from airflow.decorators import dag, task
from datetime import datetime
import os
import re
import pandas as pd
from pymongo import MongoClient
from py2neo import Graph, Node
import matplotlib.pyplot as plt

# --- Service Connection Info (from docker-compose env) ---
MONGO_HOST = os.environ.get("MONGO_HOST", "mongo")
NEO4J_HOST = os.environ.get("NEO4J_HOST", "neo4j")

MONGO_URI = f"mongodb://{MONGO_HOST}:27017/"
NEO4J_URI = f"bolt://{NEO4J_HOST}:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"

# --- Data Paths (inside container) ---
RAW_DATA_PATH = '/opt/airflow/data/raw'
GOLD_DATA_PATH = '/opt/airflow/data/gold'
NBA_STATS_FILE = os.path.join(RAW_DATA_PATH, 'nba_stats.csv')
TRAILS_FILE = os.path.join(RAW_DATA_PATH, 'hiking_trails.csv')
HAZARDS_FILE = os.path.join(RAW_DATA_PATH, 'trail_hazards.csv')
REPORT_FILE = os.path.join(GOLD_DATA_PATH, 'player_trail_report.png')

# --- Helper Functions for cleaning ---
def get_distance(text):
    match = re.search(r'(\d+\.?\d*)', str(text))
    return float(match.group(1)) if match else 0.0

def get_elevation(text):
    match = re.search(r'([\d,]+)', str(text))
    return float(match.group(1).replace(',', '')) if match else 0.0

@dag(
    dag_id='nba_hiking_compatibility_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['nba', 'hiking', 'mongo', 'neo4j', 'pandas'],
)
def nba_hiking_elt_pipeline():
    """
    ELT pipeline using the specified project stack:
    1. E-L: Loads raw CSVs into MongoDB using Pandas.
    2. T: Reads from MongoDB, transforms with Pandas, and builds a Neo4j Graph.
    3. Report: Queries Neo4j and generates a Matplotlib plot.
    """

    @task
    def extract_load_to_mongo():
        """
        E-L Step: Extracts CSVs with Pandas and loads them into MongoDB.
        """
        print(f"🟢 [EL] Connecting to MongoDB at {MONGO_URI}...")
        client = MongoClient(MONGO_URI)
        db = client.nba_hiking_db
        
        # Load Players
        players_df = pd.read_csv(NBA_STATS_FILE)
        db.bronze_players.drop()
        db.bronze_players.insert_many(players_df.to_dict('records'))
        print(f"✅ Loaded {len(players_df)} players")

        # Load Trails
        trails_df = pd.read_csv(TRAILS_FILE)
        db.bronze_trails.drop()
        db.bronze_trails.insert_many(trails_df.to_dict('records'))
        print(f"✅ Loaded {len(trails_df)} trails")
        
        # Load Hazards
        hazards_df = pd.read_csv(HAZARDS_FILE)
        db.bronze_hazards.drop()
        db.bronze_hazards.insert_many(hazards_df.to_dict('records'))
        print(f"✅ Loaded {len(hazards_df)} hazards")
        
        client.close()
        print("🎉 [EL] Task Finished.")

    @task
    def transform_mongo_to_neo4j():
        """
        T Step: Reads from MongoDB, transforms with Pandas, and builds the Neo4j Graph.
        """
        print(f"🟡 [T] Connecting to MongoDB at {MONGO_URI}...")
        client = MongoClient(MONGO_URI)
        db = client.nba_hiking_db
        
        print(f"🟡 [T] Connecting to Neo4j at {NEO4J_URI}...")
        graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        
        # 1. Read data from Mongo into Pandas DataFrames
        players_df = pd.DataFrame(list(db.bronze_players.find()))
        trails_df = pd.DataFrame(list(db.bronze_trails.find()))
        hazards_df = pd.DataFrame(list(db.bronze_hazards.find()))
        client.close()

        # 2. Transform Players
        stat_cols = ['AGE', 'GP', 'MPG', 'RPG', 'APG', 'SPG', 'BPG']
        for col in stat_cols:
            players_df[col] = pd.to_numeric(players_df[col], errors='coerce').fillna(0)
            
        players_df['endurance_score'] = players_df['MPG'] * players_df['GP']
        players_df['strength_score'] = players_df['RPG'] + players_df['BPG']
        players_df['agility_score'] = players_df['SPG'] + players_df['APG']
        
        # 3. Transform Trails
        trails_df['distance_miles'] = trails_df['Distance'].apply(get_distance)
        trails_df['elevation_ft'] = trails_df['Elevation Gain'].apply(get_elevation)
        hazards_df['has_falling_risk'] = pd.to_numeric(hazards_df['Falling'], errors='coerce').fillna(0)
        
        trails_df = trails_df.merge(hazards_df[['Name', 'has_falling_risk']], left_on='Trail Name', right_on='Name', how='left')
        
        trails_df['endurance_req'] = (trails_df['distance_miles'] * 50) + (trails_df['elevation_ft'] * 0.1)
        trails_df['agility_req'] = trails_df['has_falling_risk'].apply(lambda x: 5 if x == 1 else 0)

        # 4. Load Graph into Neo4j
        print("🟡 [T] Loading transformed data into Neo4j...")
        graph.run("MATCH (n) DETACH DELETE n") # Clear old graph
        tx = graph.begin()
        
        # Create Player Nodes
        for _, row in players_df.iterrows():
            if row['NAME']:
                node = Node("Player",
                            name=row['NAME'],
                            endurance=row['endurance_score'],
                            strength=row['strength_score'],
                            agility=row['agility_score'])
                tx.create(node)
        
        # Create Trail Nodes
        for _, row in trails_df.iterrows():
            if row['Trail Name']:
                node = Node("Trail",
                            name=row['Trail Name'],
                            difficulty=row['Difficulty'],
                            distance=row['distance_miles'],
                            elevation=row['elevation_ft'],
                            endurance_req=row['endurance_req'],
                            agility_req=row['agility_req'])
                tx.create(node)
        tx.commit()

        # 5. Build Relationships
        print("🟡 [T] Building [:CAN_HIKE] relationships...")
        graph.run("""
            MATCH (p:Player), (t:Trail)
            WHERE p.endurance > t.endurance_req
              AND p.agility > t.agility_req
              AND (CASE 
                    WHEN t.difficulty = 'Difficult' THEN p.strength > 10
                    WHEN t.difficulty = 'Moderate' THEN p.strength > 5
                    ELSE TRUE 
                   END)
            MERGE (p)-[:CAN_HIKE]->(t)
        """)
        
        print("🎉 [T] Transformations Finished.")

    @task
    def query_neo4j_and_save_plot():
        """
        Report Step: Queries Neo4j for the final report
        and generates a Matplotlib plot.
        """
        print("📊 [Report] Querying Neo4j for report...")
        graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        
        result_df = graph.run("""
            MATCH (p:Player)-[:CAN_HIKE]->(t:Trail)
            RETURN p.name AS player_name, count(t) AS compatible_trails_count
            ORDER BY compatible_trails_count DESC
            LIMIT 15
        """).to_data_frame()
        
        if result_df.empty:
            print("❌ No compatibility matches found. Stopping report.")
            return

        print(f"📊 [Report] Generating Matplotlib plot at {REPORT_FILE}...")
        os.makedirs(GOLD_DATA_PATH, exist_ok=True)
        
        plt.figure(figsize=(12, 8))
        plt.barh(result_df['player_name'], result_df['compatible_trails_count'], color='skyblue')
        plt.xlabel('Number of Compatible Trails')
        plt.ylabel('NBA Player')
        plt.title('Top 15 NBA Players by Hiking Trail Compatibility')
        plt.gca().invert_yaxis() # Show top player at the top
        plt.tight_layout()
        plt.savefig(REPORT_FILE)
        
        print(f"✅ Report plot saved to {REPORT_FILE}")

    # Define pipeline dependencies
    extract_load_to_mongo() >> transform_mongo_to_neo4j() >> query_neo4j_and_save_plot()

nba_hiking_elt_pipeline()