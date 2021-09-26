#!/bin/bash

pg_ctl start > /dev/null
createdb default > /dev/null

gender=${gender:-men}
format=${format:-test}
min_runs=${min_runs:-100}

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

SELECT runs AS score, COUNT(*) AS count
FROM innings
WHERE runs > ${min_runs}
GROUP BY runs
ORDER BY count DESC
LIMIT 25;

DROP TABLE innings;
SQL

echo $sql | psql -q -d default

dropdb default > /dev/null
pg_ctl stop > /dev/null
