#! /usr/bin/env python3

def analyse(session):
    query = """
    CALL gds.nodeSimilarity.stream('blogsGraph', {topK:30, similarityMetric:"COSINE"})
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
      // Limit the results to the top 5 most similar blogs for each blog
      WITH blog1_id, blog1_likes, COLLECT([blog2_id, similarity])[0..20] AS top20SimilarBlogs
      UNWIND top20SimilarBlogs AS top20
      RETURN blog1_id AS _id, 
             blog1_likes AS likes, 
             COLLECT({ _id: top20[0], similarity: top20[1] }) AS similarBlogs
      ORDER BY likes DESC;
    """

    projection = """
    CALL gds.graph.project(
      'blogsGraph', 
      ['Blog', 'User'],
      ['LIKED_BY']
    );
    """

    #generate fresh projection
    if list(session.run("CALL gds.graph.exists('blogsGraph')"))[0]['exists']:
        session.run("CALL gds.graph.drop('blogsGraph');")
    session.run(projection)

    return {r['_id']: r['similarBlogs'] for r in session.run(query)}