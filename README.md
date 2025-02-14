# Purpose
Ingest user blog data (https://lichess.org/blog/community) from mongodb
Generate graph database in neo4j containing blogs, and the users that liked them
Provide recommendations of:
- Footer of blog recommendations: blogs liked by users who also liked the current blog
- Personal blog recommendations: blogs liked by users who also liked blogs that were liked by the current user

# Requirements
- mongodb (8.0.4)
- neo4j (5.26.1)
	- GDS library (2.13.2)
- python3 (3.12.3)
	+ requirements.txt

# Notes
create fake blog data
python3 lila-db-seed/spamdb/spamdb.py --ublog-posts=1000

export fake blog data (if desired - but currently querying from mongodb directly)
mongoexport --db lichess --collection ublog_post --out blogs.json --jsonArray

installation tips

Download community deb here https://neo4j.com/deployment-center/

Neo4j version 5.x
wget -O - https://debian.neo4j.com/neotechnology.gpg.key | sudo apt-key add -
echo 'deb https://debian.neo4j.com stable 5' | sudo tee /etc/apt/sources.list.d/neo4j.list
sudo apt-get update

download the jar from https://github.com/neo4j/graph-data-science/releases
and follow https://neo4j.com/docs/graph-data-science/2.15/installation/neo4j-server/#installation-neo4j-server-custom