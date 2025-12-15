# Report for the Data Engineering Project

## Motivation

The idea for this pipeline was born from the intersection of our distinct hobbies. Sebastian is a basketball enthusiast who closely follows NBA player statistics, while Luis is an avid hiker who enjoys exploring nature trails. We thought it would be interesting to merge these two worlds to answer a fun question: *Which NBA players are physically best suited for specific hiking trails?*

## 1. Data Ingestion

Our ingestion process pulls data from three primary CSV sources located in our raw data directory.

- `NBA_stats_data.csv`: Contains player stats.
- `HikingTrails_TheGorge.csv`: Contains trail metadata.
- `Trail_hazards_danger.csv`: Contains specific hazard info.

### 1.1 Ingestion Process

We found the ingestion phase to be quite straightforward. The databases and CSV files were easily accessible on the internet, so we did not face significant hurdles in acquiring the data. We chose to keep the data in CSV format for the raw layer because it is universally compatible and easy to inspect using standard tools like VS Code or Excel before processing.

In our Airflow DAGs, specifically the `load_raw_csvs_to_postgres` task, we utilize Pandas to read these files and the `PostgresHook` to load them directly into our database for persistence.

## 2. Data Staging & Transformation

Our pipeline logic (ELT) is designed to clean raw text data and compute derived metrics to determine compatibility.

### 2.1 Data Cleaning and Wrangling

The hiking data required significant cleaning using Regular Expressions (Regex) because the raw CSVs contained mixed text and numbers (e.g., "1,200 feet" or "4.5 miles").
- **Trails**: We stripped non-numeric characters to convert Distance and Elevation Gain into usable floats.
- **Hazards**: We normalized hazard flags (converting "Yes", "y", "True" to binary integers).
- **NBA Players**: We handled null values in stats like Minutes Per Game (MPG) or Games Played (GP) to ensure our score calculations didn't fail.

### 2.2 Architectural Choices

- **Metric Calculation**: We decided to create composite scores rather than using raw stats directly.
    - *Endurance*: Calculated via Minutes Per Game × Games Played.
    - *Strength*: Calculated via Rebounds + Blocks.
    - *Agility*: Calculated via Steals + Assists.
- **Cartesian Product**: To find the best matches, we performed a cross-join between Players and Trails. While computationally expensive for massive datasets, it was perfectly efficient for the size of our data and allowed us to evaluate every possible Player-Trail combination.
- **Persistence**: We verify our data by loading it into Postgres tables (`hiking_trails_thegorge_raw`, `nba_stats_data_raw`) so it can be queried via tools like pgAdmin.
- **Orchestration**: We separated our concerns into three distinct DAGs:
    1. `nba_hiking_compatibility_pipeline_pandas_only`: The main pipeline for general compatibility.
    2. `rookie_hiker_dag`: Specifically filters for the 2022-23 Rookie class.
    3. `nba_teams_dag`: Aggregates data by Team to find the "Top 5 Hiking Teams."

## 3. Production Phase

### 3.1 Visualization
For the presentation layer ("Gold" data), we chose to generate static reports using `matplotlib`. The pipeline outputs visual bar charts (e.g., `player_trail_report.png`, `top_5_hiking_teams.png`) directly to the `data/gold` directory. This allows for quick visual verification of the results without needing a complex frontend.

## 4. Difficulties

### 4.1 Parsing Data
While ingestion was easy, parsing the trail data was slightly tricky due to inconsistent formatting in the "Distance" and "Elevation" columns. We had to implement robust helper functions (`get_distance`, `get_elevation`) to handle edge cases where data was missing or malformed.

### 4.2 Docker Configuration
Ensuring the Postgres container was accessible from the host machine (for verification via pgAdmin) required careful configuration of the `docker-compose.yml` ports and environment variables.