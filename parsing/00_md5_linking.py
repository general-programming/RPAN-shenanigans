import os
from collections import defaultdict

RAWS_FOLDER = "/mnt/gp_files/reddit_rpan/data/gp_rpan_archive/raws_by_hash"
os.makedirs(RAWS_FOLDER, exist_ok=True)
hashes = defaultdict(lambda: set())

with open("md5sums", "r") as f:
    for line in f:
        filehash, filename = line.strip().split("  ")
        hashes[filehash].add(filename.lstrip("./"))

print(len(hashes), "unique files")
print(f"{sum(len(x) for x in hashes.values())} total files")

for filehash, files in hashes.items():
    for filename in files:
        filepath = os.path.join("/mnt/gp_files/reddit_rpan/data/gp_rpan_archive", filename)
        rawpath = os.path.join(RAWS_FOLDER, filehash)

        try:
            os.link(filepath, rawpath)
        except FileNotFoundError:
            print(filepath, "not found")
        except FileExistsError:
            pass