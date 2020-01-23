import os

from glob import glob
from collections import defaultdict

from sqlalchemy import or_
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from rpandumper.model import sm, Stream
from rpandumper.common import create_praw

db = sm()
reddit = create_praw()

RAWS_FOLDER = "/mnt/gp_files/reddit_rpan/data/gp_rpan_archive"

for fullpath in glob(RAWS_FOLDER + "/raws_day*/*"):
    splits = fullpath.split("/")
    day_split = splits[-2]
    videofolder_split = splits[-1]

    videofolder = os.path.join(day_split, videofolder_split)

    # Ignore folders that do not start with t3.
    if not day_split.startswith("t3_"):
        pass

    post_id = "t3_" + videofolder_split.split("_")[1]
    hls_id = videofolder_split.split("_")[2]

    try:
        stream = db.query(Stream).filter(or_(Stream.post_id == post_id, Stream.hls_id == hls_id)).one()
    except NoResultFound:
        submission = reddit.submission(id=videofolder_split.split("_")[1])
        if submission.author:
            authorname = submission.author.name
        else:
            authorname = "unknownUser-UnavailableRedditor"
    except MultipleResultsFound as e:
        print(f"Multiple results found for post ID {post_id} or hls id {hls_id}")
        raise e

        stream = Stream(
            author=authorname,
            post_id=post_id,
            title=submission.title,
            hls_id=hls_id,
        )
        db.add(stream)
        db.flush()
        print("Added lost post", post_id, hls_id)

    stream.raw_foldername = videofolder
    stream.files = [x.split("/")[-1] for x in glob(fullpath + "/*")]
    db.commit()
    print(stream.id, f"{stream.title} by {stream.author}")

