#!/usr/bin/env python3
"""
etl.py
Run: python etl.py --db sqlite:///movies.db --movies path/to/movies.csv --ratings path/to/ratings.csv
"""

import os
import time
import json
import argparse
import requests
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
OMDB_API_KEY = os.getenv("OMDB_API_KEY")
if not OMDB_API_KEY:
    raise SystemExit("OMDB_API_KEY not found in environment. Set it before running.")

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

def omdb_lookup(title, year=None):
    """Lookup OMDb metadata for given title (+ optional year). Uses caching."""
    safe_name = f"{title}__{year}" if year else title
    fname = CACHE_DIR / (safe_name.replace("/", "_") + ".json")
    if fname.exists():
        return json.loads(fname.read_text())

    params = {"apikey": OMDB_API_KEY, "t": title}
    if year:
        params["y"] = str(year)
    # simple retry/backoff
    for attempt in range(4):
        try:
            r = requests.get("http://www.omdbapi.com/", params=params, timeout=8)
            data = r.json()
            fname.write_text(json.dumps(data))
            return data
        except Exception as e:
            wait = 2 ** attempt
            time.sleep(wait)
    return {"Response": "False", "Error": "Failed after retries"}

def parse_genres(genre_str):
    if not genre_str or pd.isna(genre_str):
        return []
    # MovieLens genres are 'Action|Adventure|...'
    if "|" in genre_str:
        return [g.strip() for g in genre_str.split("|") if g.strip() and g.strip() != "(no genres listed)"]
    # OMDb returns comma separated
    return [g.strip() for g in genre_str.split(",") if g.strip()]

def connect_db(db_uri):
    engine = create_engine(db_uri, echo=False, future=True)
    return engine

def ensure_schema(engine):
    # Run schema.sql
    with open("schema.sql", "r") as f:
        sql = f.read()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

def upsert_movie(conn, movie_unique_id, title, year, runtime, plot, box_office):
    # SQLite approach: try insert, on conflict ignore, then update fields
    # Simpler: check if exists then update or insert
    q = text("SELECT id FROM movies WHERE movie_id = :mid OR title = :title")
    r = conn.execute(q, {"mid": movie_unique_id, "title": title}).fetchone()
    if r:
        movie_db_id = r[0]
        conn.execute(text("""
            UPDATE movies SET year=:year, runtime_minutes=:runtime, plot=:plot, box_office=:box_office
            WHERE id=:id
        """), {"year": year, "runtime": runtime, "plot": plot, "box_office": box_office, "id": movie_db_id})
        return movie_db_id
    else:
        res = conn.execute(text("""
            INSERT INTO movies (movie_id, title, year, runtime_minutes, plot, box_office)
            VALUES (:movie_id, :title, :year, :runtime, :plot, :box_office)
        """), {"movie_id": movie_unique_id, "title": title, "year": year,
               "runtime": runtime, "plot": plot, "box_office": box_office})
        return res.lastrowid

def get_or_create_genre(conn, name):
    r = conn.execute(text("SELECT id FROM genres WHERE name=:name"), {"name": name}).fetchone()
    if r:
        return r[0]
    res = conn.execute(text("INSERT INTO genres (name) VALUES (:name)"), {"name": name})
    return res.lastrowid

def link_movie_genre(conn, movie_db_id, genre_id):
    try:
        conn.execute(text("INSERT INTO movie_genres (movie_id, genre_id) VALUES (:m,:g)"), {"m": movie_db_id, "g": genre_id})
    except IntegrityError:
        pass  # already linked

def get_or_create_director(conn, name):
    r = conn.execute(text("SELECT id FROM directors WHERE name=:name"), {"name": name}).fetchone()
    if r:
        return r[0]
    res = conn.execute(text("INSERT INTO directors (name) VALUES (:name)"), {"name": name})
    return res.lastrowid

def link_movie_director(conn, movie_db_id, director_id):
    try:
        conn.execute(text("INSERT INTO movie_directors (movie_id, director_id) VALUES (:m,:d)"), {"m": movie_db_id, "d": director_id})
    except IntegrityError:
        pass

def load_ratings(conn, ratings_df):
    # Ratings primary key is (user_id, movie_ml_id) per schema
    for idx, row in tqdm(ratings_df.iterrows(), total=len(ratings_df), desc="Loading ratings"):
        conn.execute(text("""
            INSERT OR REPLACE INTO ratings (user_id, movie_ml_id, rating, timestamp)
            VALUES (:u, :m, :r, :ts)
        """), {"u": int(row.userId), "m": int(row.movieId), "r": float(row.rating), "ts": int(row.timestamp)})

def main(db_uri, movies_csv, ratings_csv):
    engine = connect_db(db_uri)
    ensure_schema(engine)

    movies_df = pd.read_csv(movies_csv)   # columns: movieId,title,genres
    ratings_df = pd.read_csv(ratings_csv) # columns: userId,movieId,rating,timestamp

    with engine.connect() as conn:
        # 1) load ratings (simple idempotent insert/replace)
        load_ratings(conn, ratings_df)

        # 2) iterate movies and enrich with OMDb
        for _, row in tqdm(movies_df.iterrows(), total=len(movies_df), desc="Processing movies"):
            ml_id = str(row.movieId)
            title_full = row.title            # often "Toy Story (1995)"
            # parse title and year
            title = title_full
            year = None
            if title_full.endswith(')'):
                try:
                    parts = title_full.rsplit('(', 1)
                    title = parts[0].strip()
                    year = int(parts[1].rstrip(')'))
                except:
                    title = title_full

            # 1) call OMDb (cache used inside)
            omdb = omdb_lookup(title, year)
            if omdb.get("Response") == "True":
                imdb_id = omdb.get("imdbID")
                runtime = None
                try:
                    if omdb.get("Runtime") and "min" in omdb.get("Runtime"):
                        runtime = int(omdb.get("Runtime").split()[0])
                except:
                    runtime = None
                plot = omdb.get("Plot")
                box_office = omdb.get("BoxOffice")
                omdb_genres = omdb.get("Genre")  # comma separated
                director_field = omdb.get("Director")  # could be "Name1, Name2"
            else:
                imdb_id = None
                runtime = None
                plot = None
                box_office = None
                omdb_genres = None
                director_field = None

            # 2) insert/update movie
            movie_unique_id = imdb_id or f"ML_{ml_id}"
            movie_db_id = upsert_movie(conn, movie_unique_id, title, year, runtime, plot, box_office)

            # 3) genres: combine MovieLens genres & OMDb
            ml_genres = parse_genres(row.genres)
            omdb_genres_list = parse_genres(omdb_genres) if omdb_genres else []
            combined_genres = list(dict.fromkeys(ml_genres + omdb_genres_list))  # keep order, dedupe
            for g in combined_genres:
                gid = get_or_create_genre(conn, g)
                link_movie_genre(conn, movie_db_id, gid)

            # 4) directors
            if director_field and director_field != "N/A":
                director_names = [d.strip() for d in director_field.split(",") if d.strip()]
                for dn in director_names:
                    did = get_or_create_director(conn, dn)
                    link_movie_director(conn, movie_db_id, did)

        conn.commit()
    print("Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="sqlite:///movies.db", help="SQLAlchemy DB URI")
    parser.add_argument("--movies", default="movies.csv")
    parser.add_argument("--ratings", default="ratings.csv")
    args = parser.parse_args()
    main(args.db, args.movies, args.ratings)
