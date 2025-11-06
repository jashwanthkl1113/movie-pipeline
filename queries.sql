-- 1) Which movie has the highest average rating?
SELECT m.title, AVG(r.rating) as avg_rating, COUNT(*) as num_ratings
FROM movies m
JOIN ratings r ON m.movie_id = COALESCE((SELECT movie_id FROM movies WHERE movie_id LIKE 'ML_%' LIMIT 1), m.movie_id)
-- NOTE: better join: we should store movie_ml_id in movies or link by title.
-- Assuming we connected by mapping, a simpler approach:
SELECT m.title, AVG(r.rating) AS avg_rating, COUNT(*) AS num_ratings
FROM movies m
JOIN ratings r ON r.movie_ml_id = (SELECT movie_ml_id FROM ratings WHERE movie_ml_id = r.movie_ml_id) -- placeholder
GROUP BY m.title
HAVING COUNT(*) >= 5
ORDER BY avg_rating DESC
LIMIT 1;

-- More robust approach: if you stored ML id in movies table (recommended), use:
SELECT m.title, AVG(r.rating) AS avg_rating, COUNT(*) AS num_ratings
FROM movies m
JOIN ratings r ON r.movie_ml_id = m.movie_ml_id
GROUP BY m.id, m.title
HAVING COUNT(*) >= 5
ORDER BY avg_rating DESC
LIMIT 1;

-- 2) Top 5 genres by highest average rating
SELECT g.name, AVG(r.rating) AS avg_rating, COUNT(*) AS num_ratings
FROM genres g
JOIN movie_genres mg ON mg.genre_id = g.id
JOIN movies m ON m.id = mg.movie_id
JOIN ratings r ON r.movie_ml_id = m.movie_ml_id
GROUP BY g.id, g.name
HAVING COUNT(*) >= 20
ORDER BY avg_rating DESC
LIMIT 5;

-- 3) Director with the most movies
SELECT d.name, COUNT(md.movie_id) AS movie_count
FROM directors d
JOIN movie_directors md ON md.director_id = d.id
GROUP BY d.id, d.name
ORDER BY movie_count DESC
LIMIT 1;

-- 4) Average rating of movies released each year
SELECT m.year, AVG(r.rating) AS avg_rating, COUNT(*) AS num_ratings
FROM movies m
JOIN ratings r ON r.movie_ml_id = m.movie_ml_id
GROUP BY m.year
ORDER BY m.year;
