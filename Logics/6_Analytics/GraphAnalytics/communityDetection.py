from graphdatascience import GraphDataScience

# Create a connection to the Neo4j database.
gds = GraphDataScience("bolt://localhost:7687", auth=("neo4j", "password"), database='neo4j')

G = gds.graph.get('myProjection')

gds.louvain.mutate(G, mutateProperty="louvainId")
gds.wcc.mutate(G, mutateProperty="wccId")
gds.pageRank.mutate(G, mutateProperty="pageRank")

# query to get nodes with ids
# MATCH (n:Author) WHERE ID(n) IN idsList RETURN n LIMIT 25

# returns [nodeId,nodeProperty,propertyValue]
gds.graph.nodeProperties.stream(G, ['louvainId', 'wccId', 'pageRank'])