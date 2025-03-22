#! /usr/bin/env python3

from flask import Flask
from pymongo import MongoClient
from neo4j import GraphDatabase
import threading
import logging

import importer
import analyser

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

# establish connections
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['lichess']
mongo_collection = mongo_db["ublog_post"]

neo4j_url = "bolt://localhost:7687"
neo4j_username = "neo4j"
neo4j_password = "your_neo4j_password"

neo4j_driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_username, neo4j_password))
neo4j_session = neo4j_driver.session()

# set up API routes
app = Flask(__name__)

@app.route('/similarblogs/<blog_id>', methods=['GET'])
def similar_blogs(blog_id):
    return similarities.get(blog_id)

# set up mongodb change streams (requires replica set mongo) and update neo4j
# fields used will always be present https://github.com/lichess-org/lila/blob/fancy-bots/modules/ublog/src/main/UblogPost.scala#L9-L29
def watch_blogs():
    logger.info("Watching for new blogs...")
    change_stream = mongo_collection.watch([{'$match': {'operationType': 'insert'}}])

    for change in change_stream:
        logger.info(change)
        neo4j_session.execute_write(importer.blog_upsert, change['fullDocument'])

def watch_likes():
    logger.info("Watching for changes in the likes collection...")
    change_stream = mongo_collection.watch([{'$match': {'operationType': 'update'}}])

    for change in change_stream:
        logger.info(change)
        updated_fields = change['updateDescription']['updatedFields']
        for k in updated_fields:
            if k.startswith('likers'):
                likers = updated_fields[k]
                break
        neo4j_session.execute_write(importer.likes_update, change['documentKey'], likers) # doesn't edit like count

if __name__ == "__main__":

    # Full database load
    # importer.load_all_mongo_to_neo4j(neo4j_session, mongo_collection)

    # Generate graph projection and get blog: similar blogs, similarity
    similarities = analyser.analyse('server')

    # Run the Flask app
    threading.Thread(target=lambda: app.run(debug=True, use_reloader=False)).start()
    
    blog_thread = threading.Thread(target=watch_blogs)
    like_thread = threading.Thread(target=watch_likes)

    blog_thread.start()
    like_thread.start()

    blog_thread.join()
    like_thread.join()