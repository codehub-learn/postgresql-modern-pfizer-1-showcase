-- =====================================================
-- FULL-TEXT SEARCH DML
-- =====================================================

-- =====================================================
-- FULL-TEXT SEARCH basic ts_debug & to_tsvector functions
-- =====================================================
-- Analyzing tokenization and normalization with ts_debug
SELECT token, description, lexemes, dictionary
FROM ts_debug('5 explorers are traveling to a distant galaxy');

-- Using Russian configuration for full-text search
SELECT token, description, lexemes, dictionary
FROM ts_debug('greek', 'Όσα δε φτάνει η αλεπού τα κάνει κρεμαστάρια');

-- Using to_tsvector function
SELECT *
FROM to_tsvector('The explorers must save the fragile peace between Earth and the aliens.');

-- Using to_tsvector function for concatenated text string
SELECT *
FROM to_tsvector('Space Explorers' || ' ' ||
                 'The explorers must save the fragile peace between Earth and the aliens.');

-- Generating lexemes for title and description separately
SELECT title_lexemes, description_lexemes
FROM to_tsvector('Space Explorers') as title_lexemes,
     to_tsvector('The explorers must save the fragile peace between Earth and the aliens.'
     ) as description_lexemes;

-- Adding stored generated column for lexemes
ALTER TABLE omdb.movies
    ADD COLUMN lexemes tsvector
        GENERATED ALWAYS AS (to_tsvector('english', coalesce(name, '') || ' ' || coalesce(description, ''))) STORED;

-- Executing first full-text search query
SELECT id, name
FROM omdb.movies
WHERE lexemes @@ plainto_tsquery('a computer animated film');

-- Using combination of AND and OR operators
SELECT id, name
FROM omdb.movies
WHERE lexemes @@ to_tsquery('computer & animated & (lion | clownfish | donkey)');

-- Using the NOT operator and filtering by phrase
SELECT id, name
FROM omdb.movies
WHERE lexemes @@ to_tsquery('lion & !''The Lion King''');

-- Returning movies containing word “ghosts”
SELECT id, name, vote_average
FROM omdb.movies
WHERE lexemes @@ to_tsquery('ghosts')
ORDER BY vote_average DESC NULLS LAST
LIMIT 10;

-- Ranking search result with the ts_rank function
SELECT id, name, vote_average, ts_rank(lexemes, to_tsquery('ghosts')) AS search_rank
FROM omdb.movies
WHERE lexemes @@ to_tsquery('ghosts')
ORDER BY search_rank DESC, vote_average DESC NULLS LAST
LIMIT 10;

-- Assigning weights with the setweight function
SELECT id,
       name,
       description,
       (setweight(to_tsvector('english', coalesce(name, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(description, '')), 'B')) as lexemes_with_weight
FROM omdb.movies
WHERE id = 251;

-- Recreating stored generated column for lexemes
ALTER TABLE omdb.movies
    DROP COLUMN lexemes;

ALTER TABLE omdb.movies
    ADD COLUMN lexemes tsvector
        GENERATED ALWAYS AS (setweight(to_tsvector('english', coalesce(name, '')), 'A') ||
                             setweight(to_tsvector('english', coalesce(description, '')), 'B')
            ) STORED;

-- Using ts_headline to highlight search result
SELECT id,
       name,
       description,
       ts_headline(description, to_tsquery('pirates')) AS fragments,
       ts_rank(lexemes, to_tsquery('pirates'))         AS rank
FROM omdb.movies
WHERE lexemes @@ to_tsquery('pirates:B')
ORDER BY rank DESC
LIMIT 10;

-- Customizing ts_headline to show additional fragments
SELECT id,
       name,
       description,
       ts_headline(description, to_tsquery('pirates'),
                   'MaxFragments=3, MinWords=5, MaxWords=10, FragmentDelimiter=<ft_end>') AS fragments,
       ts_rank(lexemes, to_tsquery('pirates'))                                            AS rank
FROM omdb.movies
WHERE lexemes @@ to_tsquery('pirates:B')
ORDER BY rank DESC
LIMIT 1;

-- Creating GiST index over tsvector lexemes
CREATE INDEX idx_movie_lexemes_gist ON omdb.movies USING GIST (lexemes);

-- Covering Index (B-Tree)
CREATE INDEX idx_movies_release_vote
    ON omdb.movies (release_date, vote_average DESC)
    INCLUDE (name);

-- Partial Index
CREATE INDEX idx_movies_popular
    ON omdb.movies (vote_average DESC)
    WHERE vote_average >= 8.0;

-- BONUS
-- Row-Level Security (RLS) Setup
ALTER TABLE omdb.movies
    ENABLE ROW LEVEL SECURITY;
-- Don't exclude the table owner, superuser, or a role with BYPASSRLS
ALTER TABLE omdb.movies
    FORCE ROW LEVEL SECURITY;

-- RLS Policy Example
CREATE POLICY movies_recent_only
    ON omdb.movies
    FOR SELECT
    USING (release_date >= '2000-01-01' :: date);

-- Test RLS Policy
SELECT *
FROM omdb.movies;

-- Remove the policy and disable row-level security
DROP POLICY movies_recent_only ON omdb.movies;
ALTER TABLE omdb.movies
    DISABLE ROW LEVEL SECURITY;
