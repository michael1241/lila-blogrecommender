#! /usr/bin/env python3

from tqdm import tqdm
import logging
logger = logging.getLogger(__name__)

def blog_upsert(tx, blog):
    # Create Blog node
    tx.run("""
    MERGE (b:Blog {id: $blog_id})
    SET b.title = $title,
        b.likes = $likes,
        b.author = $author
    """, 
    blog_id=blog["_id"],
    title=blog["title"],
    likes=blog["likes"],
    author=blog["blog"].split(':')[1]
    )

    # Create likers and LIKED_BY relationships
    for liker in blog["likers"]:
        tx.run("""
        MERGE (l:User {username: $liker})
        WITH l, $blog_id AS blog_id
        MATCH (b:Blog {id: blog_id})
        MERGE (l)<-[:LIKED_BY]-(b)
        """, 
        liker=liker,
        blog_id=blog["_id"])

def likes_update(tx, blog, likers):
    for liker in likers:
        tx.run("""
        MERGE (l:User {username: $liker})
        WITH l, $blog_id AS blog_id
        MATCH (b:Blog {id: blog_id})
        MERGE (l)<-[:LIKED_BY]-(b)
        """, 
        liker=liker,
        blog_id=blog["_id"])

def load_all_mongo_to_neo4j(session, mongo_collection):

    blog_count = mongo_collection.count_documents({"likes": { "$gt": 0 }})

    # Create indexes for User.username and Blog.id
    session.run("CREATE TEXT INDEX username_index IF NOT EXISTS FOR (u:User) ON (u.username);")
    session.run("CREATE TEXT INDEX blog_index IF NOT EXISTS FOR (b:Blog) ON (b.id);")

    # Pull data from MongoDB (streamed)
    blog_posts = mongo_collection.find({"likes": { "$gt": 0 }}, {"_id": 1, "title": 1, "likes": 1, "likers": 1, "blog": 1})

    for blog in tqdm(blog_posts, total=blog_count):
        session.execute_write(blog_upsert, blog)

    logger.info(f"Data ingestion of {blog_count} blogs from from MongoDB to Neo4j completed.")