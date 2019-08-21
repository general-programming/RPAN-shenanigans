import praw
import os
import time

def create_praw() -> praw.Reddit:
    extra_args = {}

    if "REDDIT_USERNAME" in os.environ and "REDDIT_PASSWORD" in os.environ:
        extra_args["username"] = os.environ["REDDIT_USERNAME"]
        extra_args["password"] = os.environ["REDDIT_PASSWORD"]
        print("Got username/pw")

    return praw.Reddit(
        client_id=os.environ["REDDIT_PUBLIC"],
        client_secret=os.environ["REDDIT_SECRET"],
        redirect_uri='http://localhost:8080',
        user_agent='RPAN scraper by u/nepeat',
        **extra_args
    )

class IngestItem:
    __slots__ = ["tag", "time", "data"]
    def __init__(self, tag: str, data: str):
        self.tag = tag
        self.data = data

        self.time = int(time.time())
