DROP TABLE IF EXISTS altars;
CREATE TABLE altars
(
    player         VARCHAR(255),
    game_date_key  INTEGER,
    game_at        TIMESTAMP,
    turn           INTEGER,
    branch         VARCHAR(255),
    branch_level   INTEGER,
    note           VARCHAR(255),
    ornamentation  VARCHAR(255),
    god            VARCHAR(255)
);