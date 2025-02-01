#! /usr/bin/env python3

from pymongo import MongoClient
from neo4j import GraphDatabase

# MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['lichess']
collection = mongo_db["ublog_post"]

# Neo4j connection
neo4j_url = "bolt://localhost:7687"
neo4j_username = "neo4j"
neo4j_password = "your_neo4j_password"
neo4j_db_name = "neo4j"

def create_graph(tx, blog):
    # Create Blog node
    tx.run("""
    MERGE (b:Blog {id: $blog_id})
    SET b.title = $title,
        b.likes = $likes
    """, 
    blog_id=blog["_id"],
    title=blog["title"],
    likes=blog["likes"]
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

def main():
    neo4j_driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_username, neo4j_password))
    
    with neo4j_driver.session() as session:
        # Pull data from MongoDB (streamed)
        blog_posts = collection.find()
        # only import blogs with at least 1 like?

        for blog in blog_posts:
            session.execute_write(create_graph, blog)

    print("Data migration from MongoDB to Neo4j completed.")

if __name__ == "__main__":
    main()