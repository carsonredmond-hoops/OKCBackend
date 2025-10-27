#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import nest_asyncio
import uvicorn
import asyncio
import subprocess
import os

nest_asyncio.apply()

# Database connection
DB_NAME = "okc"
DB_USER = "postgres"
DB_PASSWORD = "okcpass"
DB_HOST = "localhost"
DB_PORT = "5432"

TEAMS_JSON_PATH = r"C:\Users\carso\Downloads\teams.json"
PLAYERS_JSON_PATH = r"C:\Users\carso\Downloads\players.json"
GAMES_JSON_PATH = r"C:\Users\carso\Downloads\games.json"
PLAYER_DATA_JSON_PATH = r"C:\Users\carso\Downloads\players.json" 
DB_EXPORT_PATH = r"C:\Users\carso\Downloads\dbexport.pgsql"
PG_DUMP_PATH = r"C:\Program Files\PostgreSQL\18\pgAdmin 4\runtime\pg_dump.exe"

# Connect to database
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cur = conn.cursor()

# Drop and rebuild tables
cur.execute("DROP TABLE IF EXISTS shot CASCADE;")
cur.execute("DROP TABLE IF EXISTS pass CASCADE;")
cur.execute("DROP TABLE IF EXISTS turnover CASCADE;")
cur.execute("DROP TABLE IF EXISTS game CASCADE;")
cur.execute("DROP TABLE IF EXISTS player CASCADE;")
cur.execute("DROP TABLE IF EXISTS team CASCADE;")

cur.execute("""
CREATE TABLE team (
    id SERIAL PRIMARY KEY,
    team_id INT UNIQUE,
    name TEXT UNIQUE
);

CREATE TABLE player (
    id SERIAL PRIMARY KEY,
    player_id INT UNIQUE,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT,
    team_id INT REFERENCES team(team_id)
);

CREATE TABLE game (
    id SERIAL PRIMARY KEY,
    game_id INT UNIQUE,
    date DATE,
    home_team_id INT REFERENCES team(team_id),
    away_team_id INT REFERENCES team(team_id),
    home_score INT,
    away_score INT,
    home_rebounds INT,
    away_rebounds INT,
    home_assists INT,
    away_assists INT
);

CREATE TABLE shot (
    id SERIAL PRIMARY KEY,
    player_id INT REFERENCES player(player_id),
    action_type TEXT,
    loc_x FLOAT,
    loc_y FLOAT,
    points INT,
    game_id INT
);

CREATE TABLE pass (
    id SERIAL PRIMARY KEY,
    player_id INT REFERENCES player(player_id),
    action_type TEXT,
    start_loc_x FLOAT,
    start_loc_y FLOAT,
    end_loc_x FLOAT,
    end_loc_y FLOAT,
    is_completed BOOLEAN,
    is_potential_assist BOOLEAN,
    is_turnover BOOLEAN,
    game_id INT
);

CREATE TABLE turnover (
    id SERIAL PRIMARY KEY,
    player_id INT REFERENCES player(player_id),
    action_type TEXT,
    loc_x FLOAT,
    loc_y FLOAT,
    game_id INT
);
""")
conn.commit()

def load_json_data():
    with open(TEAMS_JSON_PATH, "r", encoding="utf-8") as f:
        teams_data = json.load(f)
    for t in teams_data:
        cur.execute("""
            INSERT INTO team (team_id, name)
            VALUES (%s, %s)
            ON CONFLICT (team_id) DO NOTHING;
        """, (t["team_id"], t["name"]))
    conn.commit()

    with open(PLAYER_DATA_JSON_PATH, "r", encoding="utf-8") as f:
        player_data = json.load(f)
    
    for p in player_data:
        name = p.get("name", "")
        first, *rest = name.split()
        last = " ".join(rest) if rest else ""
        team_id = p.get("team_id")
        player_id = p.get("player_id")
        
        cur.execute("""
            INSERT INTO player (player_id, first_name, last_name, full_name, team_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (player_id) DO NOTHING;
        """, (player_id, first, last, name, team_id))
        
        for shot in p.get("shots", []):
            cur.execute("""
                INSERT INTO shot (player_id, action_type, loc_x, loc_y, points, game_id)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (
                player_id,
                shot.get("action_type"),
                shot.get("shot_loc_x"),
                shot.get("shot_loc_y"),
                shot.get("points"),
                shot.get("game_id")
            ))
        
        for pass_data in p.get("passes", []):
            cur.execute("""
                INSERT INTO pass (player_id, action_type, start_loc_x, start_loc_y, 
                                end_loc_x, end_loc_y, is_completed, is_potential_assist, is_turnover, game_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                player_id,
                pass_data.get("action_type"),
                pass_data.get("ball_start_loc_x"),
                pass_data.get("ball_start_loc_y"),
                pass_data.get("ball_end_loc_x"),
                pass_data.get("ball_end_loc_y"),
                pass_data.get("completed_pass"),
                pass_data.get("potential_assist"),
                pass_data.get("turnover"),
                pass_data.get("game_id")
            ))
        
        # Load turnovers
        for to in p.get("turnovers", []):
            cur.execute("""
                INSERT INTO turnover (player_id, action_type, loc_x, loc_y, game_id)
                VALUES (%s, %s, %s, %s, %s);
            """, (
                player_id,
                to.get("action_type"),
                to.get("tov_loc_x"),
                to.get("tov_loc_y"),
                to.get("game_id")
            ))
    
    conn.commit()

    with open(GAMES_JSON_PATH, "r", encoding="utf-8") as f:
        games_data = json.load(f)
    for g in games_data:
        cur.execute("""
            INSERT INTO game (game_id, date, home_team_id, away_team_id, home_score, away_score,
            home_rebounds, away_rebounds, home_assists, away_assists)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id) DO NOTHING;
        """, (
            g.get("id"), g.get("date"), g.get("home_team_id"), g.get("away_team_id"),
            g.get("home_score"), g.get("away_score"),
            g.get("home_rebounds", 0), g.get("away_rebounds", 0),
            g.get("home_assists", 0), g.get("away_assists", 0)
        ))
    conn.commit()
    
    print("Data loaded successfully.")

load_json_data()

os.environ["PGPASSWORD"] = DB_PASSWORD
subprocess.run([
    PG_DUMP_PATH,
    "-U", DB_USER,
    "-d", DB_NAME,
    "-f", DB_EXPORT_PATH
], check=True)
print(f"Database exported successfully to {DB_EXPORT_PATH}")

# FastAPI
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/v1/playerSummary/{playerID}")
def get_player_summary(playerID: int):
    cur.execute("""
        SELECT player_id, first_name, last_name, full_name, team_id 
        FROM player WHERE player_id=%s
    """, (playerID,))
    player_row = cur.fetchone()
    
    if not player_row:
        raise HTTPException(status_code=404, detail="Player not found")
    
    pid, first, last, full_name, team_id = player_row
    

    response = {
        "name": full_name,
        "playerID": pid,
        "totalShotAttempts": 0,
        "totalPoints": 0,
        "totalPasses": 0,
        "totalPotentialAssists": 0,
        "totalTurnovers": 0,
        "totalPassingTurnovers": 0,
        "pickAndRollCount": 0,
        "isolationCount": 0,
        "postUpCount": 0,
        "offBallScreenCount": 0,
        "pickAndRoll": {},
        "isolation": {},
        "postUp": {},
        "offBallScreen": {}
    }
    
    for action_type_db, action_type_key in [
        ("pickAndRoll", "pickAndRoll"),
        ("isolation", "isolation"),
        ("postUp", "postUp"),
        ("offBallScreen", "offBallScreen")
    ]:

        cur.execute("""
            SELECT loc_x, loc_y, points FROM shot 
            WHERE player_id=%s AND action_type=%s
        """, (pid, action_type_db))
        shots = [{"loc": [row[0], row[1]], "points": row[2]} for row in cur.fetchall()]
        

        cur.execute("""
            SELECT start_loc_x, start_loc_y, end_loc_x, end_loc_y, 
                   is_completed, is_potential_assist, is_turnover
            FROM pass WHERE player_id=%s AND action_type=%s
        """, (pid, action_type_db))
        passes = [{
            "startLoc": [row[0], row[1]],
            "endLoc": [row[2], row[3]],
            "isCompleted": row[4],
            "isPotentialAssist": row[5],
            "isTurnover": row[6]
        } for row in cur.fetchall()]
        

        cur.execute("""
            SELECT loc_x, loc_y FROM turnover 
            WHERE player_id=%s AND action_type=%s
        """, (pid, action_type_db))
        turnovers = [{"loc": [row[0], row[1]]} for row in cur.fetchall()]
        

        total_shot_attempts = len(shots)
        total_points = sum(s["points"] for s in shots)
        total_passes = len(passes)
        total_potential_assists = sum(1 for p in passes if p["isPotentialAssist"])
        total_turnovers = len(turnovers)
        total_passing_turnovers = sum(1 for p in passes if p["isTurnover"])
        

        response[action_type_key] = {
            "totalShotAttempts": total_shot_attempts,
            "totalPoints": total_points,
            "totalPasses": total_passes,
            "totalPotentialAssists": total_potential_assists,
            "totalTurnovers": total_turnovers,
            "totalPassingTurnovers": total_passing_turnovers,
            "shots": shots,
            "passes": passes,
            "turnovers": turnovers
        }
        
        response["totalShotAttempts"] += total_shot_attempts
        response["totalPoints"] += total_points
        response["totalPasses"] += total_passes
        response["totalPotentialAssists"] += total_potential_assists
        response["totalTurnovers"] += total_turnovers
        response["totalPassingTurnovers"] += total_passing_turnovers
        
       
        count_key = f"{action_type_key}Count"
        response[count_key] = total_shot_attempts + total_passes + total_turnovers
    
    return JSONResponse(content=response)

print("Backend running at http://localhost:8000")
print("Test API: http://localhost:8000/api/v1/playerSummary/0")
config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info")
server = uvicorn.Server(config)
asyncio.get_event_loop().create_task(server.serve())

@app.get("/api/v1/players")
def get_all_players():
    cur.execute("SELECT player_id, first_name, last_name, full_name, team_id FROM player;")
    rows = cur.fetchall()
    return JSONResponse(content=[{
        "playerID": r[0],
        "firstName": r[1],
        "lastName": r[2],
        "fullName": r[3],
        "teamID": r[4]
    } for r in rows])

@app.get("/api/v1/teams")
def get_all_teams():
    cur.execute("SELECT team_id, name FROM team;")
    rows = cur.fetchall()
    return JSONResponse(content=[{"teamID": r[0], "name": r[1]} for r in rows])

@app.get("/api/v1/games")
def get_all_games():
    cur.execute("""
        SELECT game_id, date, home_team_id, away_team_id, home_score, away_score,
               home_rebounds, away_rebounds, home_assists, away_assists
        FROM game;
    """)
    rows = cur.fetchall()
    return JSONResponse(content=[{
        "gameID": r[0],
        "date": str(r[1]),
        "homeTeamID": r[2],
        "awayTeamID": r[3],
        "homeScore": r[4],
        "awayScore": r[5],
        "homeRebounds": r[6],
        "awayRebounds": r[7],
        "homeAssists": r[8],
        "awayAssists": r[9]
    } for r in rows])


# In[ ]:




