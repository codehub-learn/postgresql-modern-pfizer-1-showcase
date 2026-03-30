-- =====================================================
-- FULL-TEXT SEARCH DDL
-- =====================================================
CREATE SCHEMA IF NOT EXISTS omdb;

DROP TABLE IF EXISTS omdb.movies;

CREATE TABLE omdb.movies
(
    id           BIGINT PRIMARY KEY,
    name         TEXT NOT NULL,
    description  TEXT NOT NULL,
    release_date DATE,
    runtime      INT,
    budget       NUMERIC,
    revenue      NUMERIC,
    vote_average NUMERIC,
    votes_count  BIGINT
);
