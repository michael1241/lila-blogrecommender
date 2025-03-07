#! /usr/bin/env python3

from pymongo import MongoClient
from neo4j import GraphDatabase
from tqdm import tqdm

mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['lichess']
collection = mongo_db["ublog_post"]

neo4j_url = "bolt://localhost:7687"
neo4j_username = "neo4j"
neo4j_password = "your_neo4j_password"

def create_graph(tx, blog):
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

def main():
    neo4j_driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_username, neo4j_password))
    
    blog_count = collection.count_documents({"likes": { "$gt": 0 }})

    with neo4j_driver.session() as session:
        # Create indexes for User.username and Blog.id
        session.run("CREATE TEXT INDEX username_index IF NOT EXISTS FOR (u:User) ON (u.username);")
        session.run("CREATE TEXT INDEX blog_index IF NOT EXISTS FOR (b:Blog) ON (b.id);")

        # Pull data from MongoDB (streamed)
        blog_posts = collection.find({"likes": { "$gt": 0 }}, {"_id": 1, "title": 1, "likes": 1, "likers": 1, "blog": 1})

        for blog in tqdm(blog_posts, total=blog_count):
            session.execute_write(create_graph, blog)

    print("Data migration from MongoDB to Neo4j completed.")

if __name__ == "__main__":
    main()
