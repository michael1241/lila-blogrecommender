// generate the graph projection

CALL gds.graph.project(
  'blogsGraph', 
  ['Blog', 'User'],
  ['LIKED_BY']
);

// identify top 3 most similar blogs based on likers for each blog

CALL gds.nodeSimilarity.stream('blogsGraph')
  YIELD node1, node2, similarity
  WITH gds.util.asNode(node1).id AS blog1_id, 
       gds.util.asNode(node2).id AS blog2_id, 
       similarity
  // Group by the first blog (blog1) and collect all the similar blogs (blog2) and their similarity scores
  WITH blog1_id, COLLECT([blog2_id, similarity]) AS similarBlogs
  // Sort the collected blogs by similarity score, descending
  UNWIND similarBlogs AS pair
  WITH blog1_id, pair[0] AS blog2_id, pair[1] AS similarity
  ORDER BY blog1_id, similarity DESC
  // Limit the results to the top 3 most similar blogs for each blog
  WITH blog1_id, COLLECT([blog2_id, similarity])[0..3] AS top3SimilarBlogs
  UNWIND top3SimilarBlogs AS top3
  RETURN blog1_id, top3[0] AS blog2_id, top3[1] AS similarity
  ORDER BY blog1_id, similarity DESC;

// clear the database if required

MATCH (a) -[r] -> () DELETE a, r
MATCH (a) DELETE a

// delete the cypher projection if required

CALL gds.graph.drop('blogsGraph');