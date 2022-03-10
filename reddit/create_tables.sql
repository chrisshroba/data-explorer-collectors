-- Reddit Tables
-- reddit_submission
--   - id
--   - created_utc
--   - title
--   - url
--   - row_added
-- reddit_comment
--   - id
--   - created_utc
--   - content
--   - row_added
-- reddit_saved_post
--   - reddit_my_user_id
--   - id
--   - row_added
-- reddit_saved_comment
--   - reddit_my_user_id
--   - id
--   - row_added
-- TODO Add all the other reddit data I have access to
--
-- reddit_my_user
--   - rowid
--   - username
--   - password

CREATE TABLE reddit_submission (
  id text PRIMARY KEY,
  created_utc timestamptz,
  title text,
  url text,
  post_json jsonb,
  row_added timestamptz NOT NULL DEFAULT NOW()
);

CREATE TABLE reddit_saved_post (
  submission_id text REFERENCES reddit_submission(id) PRIMARY KEY,
  row_added timestamptz NOT NULL DEFAULT NOW()
);
