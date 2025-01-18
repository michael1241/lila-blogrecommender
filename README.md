# Purpose
Ingest user blog data (https://lichess.org/blog/community) from mongodb
Generate graph database in neo4j containing blogs, and the users that liked them
Provide recommendations of:
- Footer of blog recommendations: blogs liked by users who also liked the current blog
- Personal blog recommendations: blogs liked by users who also liked blogs that were liked by the current user

# Requirements
- mongodb (8.0.4 used)
- neo4j (5.26.1 used)
- python3 (3.12.3 used)
	+ requirements.txt

# Notes
create fake blog data
python3 lila-db-seed/spamdb/spamdb.py --ublog-posts=1000

export fake blog data (if desired - but currently querying from mongodb directly)
mongoexport --db lichess --collection ublog_post --out blogs.json --jsonArray