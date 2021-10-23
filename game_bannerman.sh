#!/bin/bash

pg_ctl start > /dev/null
createdb default > /dev/null

gender=${gender:-men}
type=${type:-game}
format=test

read -r -d '' sql <<SQL
CREATE TABLE innings (
  index integer,
  player text,
  team text,
  runs integer,
  runs_txt text,
  not_out boolean,
  mins numeric,
  bf numeric,
  fours numeric,
  sixes numeric,
  sr numeric,
  pos integer,
  innings integer,
  opposition text,
  ground text,
  start_date date
);

COPY innings
FROM '$(pwd)/data/${gender}_${format}_batting.csv'
WITH (FORMAT csv, HEADER);

WITH
outs AS (
  SELECT
    ground,
    start_date,
    player,
    team,
    runs,
    CASE WHEN not_out THEN 0 ELSE 1 END AS outs
  FROM innings
),
forty_wickets AS (
  SELECT
    ground,
    start_date,
    SUM(runs) AS game_runs
  FROM outs
  GROUP BY ground, start_date
  HAVING SUM(outs) = 40
),
twenty_wickets AS (
  SELECT
    ground,
    start_date,
    team,
    SUM(runs) AS team_runs
  FROM outs
  GROUP BY ground, start_date, team
  HAVING SUM(outs) = 20
),
game_bannermen AS (
  SELECT
    innings.ground,
    innings.start_date,
    innings.player,
    SUM(innings.runs) AS runs,
    MIN(forty_wickets.game_runs) AS game_runs,
    SUM(innings.runs)::numeric(20, 10) / MIN(forty_wickets.game_runs)::numeric(20, 10) AS proportion
  FROM innings
  INNER JOIN forty_wickets ON
    innings.ground = forty_wickets.ground AND
    innings.start_date = forty_wickets.start_date
  GROUP BY
    innings.ground,
    innings.start_date,
    innings.player
),
team_bannermen AS (
  SELECT
    innings.ground,
    innings.start_date,
    innings.team,
    innings.player,
    SUM(innings.runs) AS runs,
    MIN(twenty_wickets.team_runs) AS team_runs,
    SUM(innings.runs)::numeric(20, 10) / MIN(twenty_wickets.team_runs)::numeric(20, 10) AS proportion
  FROM innings
  INNER JOIN twenty_wickets ON
    innings.ground = twenty_wickets.ground AND
    innings.start_date = twenty_wickets.start_date AND
    innings.team = twenty_wickets.team
  GROUP BY
    innings.ground,
    innings.start_date,
    innings.team,
    innings.player
)
SELECT
  ground,
  start_date,
  player,
  runs,
  ${type}_runs,
  proportion::numeric(20, 2)
FROM ${type}_bannermen
WHERE runs IS NOT NULL
ORDER BY proportion DESC
LIMIT 10;

DROP TABLE innings;
SQL

echo "${sql}" | psql -q -d default

dropdb default > /dev/null
pg_ctl stop > /dev/null
