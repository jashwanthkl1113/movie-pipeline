-- schema.sql

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS movies (
  id INTEGER PRIMARY KEY,            -- internal id
  movie_id TEXT UNIQUE,              -- imdb id if available, else ML id (unique)
  title TEXT NOT NULL,
  year INTEGER,
  runtime_minutes INTEGER,
  plot TEXT,
  box_office TEXT,
  released_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS genres (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_genres (
  movie_id INTEGER,
  genre_id INTEGER,
  PRIMARY KEY (movie_id, genre_id),
  FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
  FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS directors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_directors (
  movie_id INTEGER,
  director_id INTEGER,
  PRIMARY KEY (movie_id, director_id),
  FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
  FOREIGN KEY (director_id) REFERENCES directors(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ratings (
  user_id INTEGER NOT NULL,
  movie_ml_id INTEGER NOT NULL,       -- MovieLens movieId
  rating REAL NOT NULL,
  timestamp INTEGER,
  PRIMARY KEY (user_id, movie_ml_id)
);
