# NBA Hiking Pipeline
5IF INSA - Foundations of data engineering project: Correlation of NBA player statistics with Hiking Trail difficulty to compute hiking compatibility.

| Student name | GitHub profile |
| :--- | :--- |
| Sebastian Pica | [\[Profile Link\]](https://github.com/SebiP21) |
| Luis Schwarz | [Profile Link](https://github.com/lsvbs) |

# Project Description

The goal of this project is to create a data pipeline that ingests NBA player performance statistics along with data regarding hiking trails in "The Gorge" area. By processing physical attributes (Endurance, Strength, Agility) derived from basketball stats and comparing them against trail requirements (Distance, Elevation, Hazards), we aim to compute a "Hiking Compatibility Score" for players.

We have implemented multiple DAGs to analyze specific subsets of data, such as top-performing Rookies and the most "outdoorsy" NBA teams.

## Project report

Project report available [here](docs/Report.md)

## Checklist

- [x] repository with the code, well documented
- [x] docker-compose file to run the environment
- [x] detailed description of the various steps
- [x] report (Can be in the Repository README) with the project design steps (divided per area)
- [x] Example dataset: the project testing works offline with provided CSVs.
- [x] slides for the project presentation.

## Data Sources:
The data used in this project is stored locally in the `data/raw` folder, consisting of:
- **NBA Stats**: Performance metrics for players (GP, MPG, Rebounds, etc.).
- **Hiking Trails**: Distance, elevation gain, and difficulty ratings for trails in The Gorge.
- **Trail Hazards**: Specific danger flags (Falling risks, etc.) for specific trails.

# Development info
## Getting started

1. Ensure you have Docker installed and running.
2. Get your computer's user id by typing in `id -u` in your bash terminal.
3. Create a `.env` file (if not present) and set `AIRFLOW_UID` to your ID.
4. Build and run the environment using the `docker-compose up` command (run it in the directory of the project).
5. You can now connect to [localhost:8080](http://localhost:8080/) to access the airflow dashboard. The user and password are `admin`.
6. Ensure you have a connection to the Postgres database configured if running the SQL operators (default is usually `postgres_default`).

## Environment info
| Service  | Address:Port           | Image        |
| :------- | ---------------------- | ------------ |
| postgres | http://localhost:5432/ | postgres:13  |
| airflow  | http://localhost:8080/ | apache/airflow:2.7.2 |
