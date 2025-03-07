#! /usr/bin/env python3

from neo4j import GraphDatabase
import csv

neo4j_url = "bolt://localhost:7687"
neo4j_username = "neo4j"
neo4j_password = "your_neo4j_password"

driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_username, neo4j_password))

def run_query(driver, query):
    with driver.session() as session:
        result = list(session.run(query))
        return result

def main():
    query = """
    CALL gds.nodeSimilarity.stream('blogsGraph', {topK:30})
      YIELD node1, node2, similarity
      WITH gds.util.asNode(node1).id AS blog1_id, 
           gds.util.asNode(node2).id AS blog2_id, 
           similarity, 
           gds.util.asNode(node1).likes AS blog1_likes,
           gds.util.asNode(node1).author AS blog1_user_id,
           gds.util.asNode(node2).author AS blog2_user_id
      // Exclude blogs written by the same user
      WHERE blog1_user_id <> blog2_user_id
      // Group by the first blog (blog1) and collect all the similar blogs (blog2) and their similarity scores
      WITH blog1_id, blog1_likes, COLLECT([blog2_id, similarity]) AS similarBlogs
      // Sort the collected blogs by similarity score, descending
      UNWIND similarBlogs AS pair
      WITH blog1_id, blog1_likes, pair[0] AS blog2_id, pair[1] AS similarity
      ORDER BY blog1_id, similarity DESC
      // Limit the results to the top 3 most similar blogs for each blog
      WITH blog1_id, blog1_likes, COLLECT([blog2_id, similarity])[0..3] AS top3SimilarBlogs
      UNWIND top3SimilarBlogs AS top3
      RETURN blog1_id AS _id, 
             blog1_likes AS likes, 
             COLLECT({ _id: top3[0], similarity: top3[1] }) AS similarBlogs
      ORDER BY likes DESC;
    """

    projection = """
    CALL gds.graph.project(
      'blogsGraph', 
      ['Blog', 'User'],
      ['LIKED_BY']
    );
    """

    run_query(driver, "CALL gds.graph.drop('blogsGraph');")
    run_query(driver, projection)
    result = run_query(driver, query)

    with open('output.csv', 'w', newline='') as csvfile:
        fieldnames = ['_id', 'likes', 'similarBlogs']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for record in result:
            writer.writerow({
                '_id': record['_id'],
                'likes': record['likes'],
                'similarBlogs': record['similarBlogs']
            })


if __name__ == "__main__":
    main()
