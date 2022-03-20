import arrow
import base64
import json
import os
import praw
import praw.models
import psycopg2.extras
import util

REDDIT_CLIENT_ID = os.environ["REDDIT_CLIENT_ID"]
REDDIT_CLIENT_SECRET = os.environ["REDDIT_CLIENT_SECRET"]
REDDIT_USER_AGENT = os.environ["REDDIT_USER_AGENT"]
REDDIT_USERNAME = os.environ["REDDIT_USERNAME"]
REDDIT_PASSWORD_BASE64 = os.environ["REDDIT_PASSWORD_BASE64"]
REDDIT_PASSWORD = base64.b64decode(
    REDDIT_PASSWORD_BASE64.encode("utf-8")
).decode("utf-8")


class RedditCollector(util.Collector):
    def get_version(self):
        return "0.1"

    def get_collector_name(self):
        return "reddit_collector"

    def get_rate_limit_str(self, reddit):
        limits = reddit.auth.limits
        remaining = int(limits["remaining"])
        used = int(limits["used"])
        reset = arrow.get(limits["reset_timestamp"]).to("local")
        now = arrow.now()
        until_seconds = (reset - now).total_seconds() if reset > now else 0
        until_minutes = int(until_seconds // 60)
        until_seconds_rem = int(until_seconds % 60)
        until_str = f"{until_minutes}m{until_seconds_rem}s"
        s = (
            f"<RateLimit remaining={remaining} used={used} reset_in="
            + f"{until_str}>"
        )
        return s

    def get_multirow_insertions(self):

        # Get already seen reddit posts
        with util.create_db_conn() as conn, util.get_cursor_for_conn(
            conn
        ) as cur:
            seen_saved_posts_query_results = util.query(
                cur, "SELECT * FROM reddit_saved_post"
            )
            seen_saved_posts = seen_saved_posts_query_results.rows
        seen_saved_post_ids = {p.submission_id for p in seen_saved_posts}

        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
            username=REDDIT_USERNAME,
            password=REDDIT_PASSWORD,
        )

        me = reddit.user.me()

        self.log(f"Current Rate Limit: {self.get_rate_limit_str(reddit)}")
        self.log(f"Logged in as {me.name}.")
        self.log("Fetching Saved Posts Now.")

        saved_posts = []
        saved_comments = []
        for idx, item in enumerate(me.saved(limit=None)):
            if isinstance(item, praw.models.Submission):
                saved_posts.append(item)
            else:
                saved_comments.append(item)

            # If we've already seen the last 10 posts, then quit early.
            if len(saved_posts) >= 10 and set(
                [p.id for p in saved_posts][-10:]
            ).issubset(set(seen_saved_post_ids)):
                self.log(
                    f"Seen {idx+1} items, and the last 10 posts have "
                    + "already been seen, so not continuing further."
                )
                break

            if idx % 100 == 99:
                self.log(f"Processed {idx+1} saved items so far.")

        unseen_saved_posts = [
            p for p in saved_posts if p.id not in seen_saved_post_ids
        ]
        self.log(
            f"Found {len(saved_posts)} saved posts, {len(unseen_saved_posts)}"
            + " of which are new."
        )

        def serialize_post(p):
            return json.dumps(
                p.__dict__, default=lambda o: "<Not Serializable>"
            )

        submission_rows_to_add = [
            (
                p.id,
                arrow.get(p.created_utc).datetime,
                p.title,
                p.url,
                serialize_post(p),
            )
            for p in unseen_saved_posts
        ]
        saved_post_rows_to_add = [(p.id,) for p in unseen_saved_posts]

        submission_insertions = util.MultiRowInsertion(
            self.INSERT_SUBMISSIONS_SQL,
            submission_rows_to_add,
            "reddit_submission",
        )
        saved_post_insertions = util.MultiRowInsertion(
            self.INSERT_SAVED_POSTS_SQL,
            saved_post_rows_to_add,
            "reddit_saved_post",
        )

        insertions = [submission_insertions, saved_post_insertions]

        self.log(f"Current Rate Limit: {self.get_rate_limit_str(reddit)}")
        return insertions

    INSERT_SUBMISSIONS_SQL = (
        "INSERT INTO reddit_submission (id, created_utc, title, url, "
        + "post_json) VALUES %s ON CONFLICT DO NOTHING"
    )

    INSERT_SAVED_POSTS_SQL = (
        "INSERT INTO reddit_saved_post (submission_id) VALUES %s ON CONFLICT"
        + " DO NOTHING"
    )


if __name__ == "__main__":
    collector = RedditCollector()
    collector.run()
