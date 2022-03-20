CREATE SCHEMA collectors;
CREATE TABLE collectors.runs (
  id serial PRIMARY KEY,
  collector_name text,
  collector_version text,
  start_time timestamptz
);
CREATE TABLE collectors.run_results (
  id serial PRIMARY KEY,
  run_id integer references collectors.runs (id),
  logs text,
  errors text,
  end_time timestamptz
);
CREATE TABLE collectors.run_rows_added (
  id serial PRIMARY KEY,
  run_id integer references collectors.runs (id),
  table_name text,
  num_rows_added integer
);
