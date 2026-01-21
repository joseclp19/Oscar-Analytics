CREATE TABLE Dim_Movie (
    film TEXT,
    year_ceremony INT,
    tmdb_id INT PRIMARY KEY,
    revenue BIGINT,
    runtime INT,
    genres TEXT,
    release_date DATE,
    poster_url TEXT,
    status TEXT
);

CREATE TABLE Fact_Award_Raw (
    year_film INT,
    year_ceremony INT,
    ceremony INT,
    category TEXT,
    canon_category TEXT,
    winner_name TEXT, 
    film_name TEXT,   
    is_winner BOOLEAN, 
    tmdb_id INT
);

CREATE TABLE Fact_Award (
    award_id SERIAL PRIMARY KEY,
    tmdb_id INT,
    year_ceremony INT,
    category TEXT,
    winner_name TEXT,
    is_winner BOOLEAN,
    FOREIGN KEY (tmdb_id) REFERENCES Dim_Movie(tmdb_id)
);

INSERT INTO Fact_Award (tmdb_id, year_ceremony, category, winner_name, is_winner)
SELECT 
    tmdb_id, 
    year_ceremony, 
    category, 
    winner_name, 
    is_winner
FROM Fact_Award_Raw
WHERE tmdb_id IS NOT NULL;

--Last 10 Academy Awards Winners Query
SELECT 
    m.film,
    f.year_ceremony,
    f.category,
    m.budget,
    m.revenue
FROM Fact_Award f
JOIN Dim_Movie m ON f.tmdb_id = m.tmdb_id
ORDER BY f.year_ceremony DESC
LIMIT 10;

--No Budget or Revenue Query
SELECT 
    film, 
    year_ceremony, 
    budget, 
    revenue,
    status
FROM Dim_Movie
WHERE budget = 0 OR revenue = 0
ORDER BY year_ceremony DESC;

--View for Power BI
CREATE VIEW vw_OscarPortfolio AS
SELECT 
    m.tmdb_id,
    m.film,
    m.year_ceremony,
    m.genres,
    m.poster_url,
    m.budget,
    m.revenue,
    (m.revenue - m.budget) AS profit_nominal,
    
    CASE 
        WHEN m.budget > 0 THEN ROUND(((m.revenue - m.budget)::numeric / m.budget) * 100, 2)
        ELSE 0 
    END AS roi_percentage,

    m.runtime,
    -- Movie Runtime Categories --
    CASE 
        WHEN m.runtime < 90 THEN 'Short (<90m)'
        WHEN m.runtime BETWEEN 90 AND 120 THEN 'Standard (90-120m)'
        WHEN m.runtime BETWEEN 121 AND 150 THEN 'Long (121-150m)'
        WHEN m.runtime > 150 THEN 'Epic (>150m)'
        ELSE 'Unknown'
    END AS duration_category,

    
    TO_CHAR(m.release_date, 'Month') as release_month_name,
    EXTRACT(MONTH FROM m.release_date) as release_month_num

FROM Dim_Movie m
WHERE m.tmdb_id IS NOT NULL;