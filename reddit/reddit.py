import base64
import json
import os
import util

import arrow
import dotenv
import praw
import praw.models
import psycopg2.extras

with util.create_db_conn() as conn, conn.cursor(
    cursor_factory=psycopg2.extras.NamedTupleCursor
) as cur:
    seen_saved_posts, _ = util.query(cur, "SELECT * FROM reddit_saved_post")
seen_saved_post_ids = {p.submission_id for p in seen_saved_posts}

print(f"Found {len(seen_saved_post_ids)} saved posts already in db")

dotenv.load_dotenv()

REDDIT_CLIENT_ID = os.environ["REDDIT_CLIENT_ID"]
REDDIT_CLIENT_SECRET = os.environ["REDDIT_CLIENT_SECRET"]
REDDIT_USER_AGENT = os.environ["REDDIT_USER_AGENT"]
REDDIT_USERNAME = os.environ["REDDIT_USERNAME"]
REDDIT_PASSWORD_BASE64 = os.environ["REDDIT_PASSWORD_BASE64"]
REDDIT_PASSWORD = base64.b64decode(
    REDDIT_PASSWORD_BASE64.encode("utf-8")
).decode("utf-8")

reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT,
    username=REDDIT_USERNAME,
    password=REDDIT_PASSWORD,
)

me = reddit.user.me()


def rate_limit_str():
    l = reddit.auth.limits
    remaining = int(l["remaining"])
    used = int(l["used"])
    reset = arrow.get(l["reset_timestamp"]).to("local")
    now = arrow.now()
    until_seconds = (reset - now).total_seconds() if reset > now else 0
    until_minutes = int(until_seconds // 60)
    until_seconds_rem = int(until_seconds % 60)
    until_str = f"{until_minutes}m{until_seconds_rem}s"
    s = f"<RateLimit remaining={remaining} used={used} reset_in={until_str}>"
    return s


print(f"Current Rate Limit: {rate_limit_str()}")

print(f"Logged in as {me.name}.")

print("Fetching Saved Posts Now.")

saved_posts = []
saved_comments = []
for idx, item in enumerate(me.saved(limit=None)):
    if isinstance(item, praw.models.Submission):
        saved_posts.append(item)
    else:
        saved_comments.append(item)
    if idx % 100 == 99:
        print(f"Processed {idx+1} saved items so far.")


unseen_saved_posts = [p for p in saved_posts if p.id not in seen_saved_post_ids]
print(
    f"Found {len(saved_posts)} saved posts, {len(unseen_saved_posts)} of which are new."
)


def serialize_post(p):
    return json.dumps(p.__dict__, default=lambda o: "<Not Serializable>")


rows_to_add = [
    (p.id, arrow.get(p.created_utc).datetime, p.title, p.url, serialize_post(p))
    for p in unseen_saved_posts
]
print(f"Inserting {len(rows_to_add)} rows.")

with util.create_db_conn() as conn, conn.cursor(
    cursor_factory=psycopg2.extras.NamedTupleCursor
) as cur:
    util.insert(
        cur,
        "INSERT INTO reddit_submission (id, created_utc, title, url, post_json) VALUES %s ON CONFLICT DO NOTHING",
        rows_to_add,
    )
    util.insert(
        cur,
        "INSERT INTO reddit_saved_post (submission_id) VALUES %s ON CONFLICT DO NOTHING",
        [(p.id,) for p in unseen_saved_posts],
    )

print("Done inserting.")
print(f"Current Rate Limit: {rate_limit_str()}")
