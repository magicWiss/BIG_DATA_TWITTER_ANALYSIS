from graphdatascience import GraphDataScience
from neo4j import GraphDatabase, Result

uri = "bolt://localhost:7687"
user = 'neo4j'
password = 'password'

driver = GraphDatabase.driver(uri, auth=(user, password), database='neo4j')

def getNodeIds(idxsList):
    pandas_df = driver.execute_query(
        f"MATCH (n:Author) WHERE ID(n) IN {idxsList} RETURN ID(n) AS nodeId, n.id AS id",
        database_="neo4j",
        result_transformer_= Result.to_df
    )
    return pandas_df.set_index('nodeId')['id'].to_dict()

def nodeidxtonodeid(field):
    if(isinstance(field, list)):
        res_list = []
        for i in field:
            res_list.append(df_nodeIdx_nodeId[i])
        return res_list
    else:
        try:
            return df_nodeIdx_nodeId[field]
        except:
            return field

# Create a connection to the Neo4j database.
gds = GraphDataScience(uri, auth=(user, password), database='neo4j')

# gds.graph.drop('myProjection')
# gds.graph.project('myProjection', 'Author', 'REPLYED_TO')

G = gds.graph.get('myProjection')

gds.louvain.mutate(G, mutateProperty="louvainId")
gds.wcc.mutate(G, mutateProperty="wccId")
gds.pageRank.mutate(G, mutateProperty="pageRank")

res_df = gds.graph.nodeProperties.stream(G, ['louvainId', 'wccId', 'pageRank'])

res_df_louvain = res_df[res_df['nodeProperty'] == 'louvainId'].drop(['nodeProperty'], axis=1)
louvainId2nodes = res_df_louvain.groupby('propertyValue')['nodeId'].agg(list).reset_index()
louvainId2nodes.columns = ['propertyValue', 'nodeIds']
louvainId2nodes['numNodes'] = louvainId2nodes['nodeIds'].apply(lambda x: len(x))
df_nodeIdx_nodeId = getNodeIds(louvainId2nodes['nodeIds'].sum())
louvainId2nodes['nodeIds'] = louvainId2nodes['nodeIds'].apply(nodeidxtonodeid)
louvainId2nodes.to_csv('D:\Projects\BIG_DATA_TWITTER_ANALYSIS\Dataset\CuratedData\CommunityDetection\louvain.csv', index=False)


res_df_wcc = res_df[res_df['nodeProperty'] == 'wccId'].drop(['nodeProperty'], axis=1)
wccId2nodes = res_df_wcc.groupby('propertyValue')['nodeId'].agg(list).reset_index()
wccId2nodes.columns = ['propertyValue', 'nodeIds']
wccId2nodes['numNodes'] = wccId2nodes['nodeIds'].apply(lambda x: len(x))
df_nodeIdx_nodeId = getNodeIds(wccId2nodes['nodeIds'].sum())
wccId2nodes['nodeIds'] = wccId2nodes['nodeIds'].apply(nodeidxtonodeid)
wccId2nodes.to_csv('D:\Projects\BIG_DATA_TWITTER_ANALYSIS\Dataset\CuratedData\CommunityDetection\wcc.csv', index=False)

res_df_pagerank = res_df[res_df['nodeProperty'] == 'pageRank'].drop(['nodeProperty'], axis=1)
df_nodeIdx_nodeId = getNodeIds(res_df_pagerank['nodeId'].agg(list))
res_df['nodeId'] = res_df['nodeId'].apply(nodeidxtonodeid)
res_df_pagerank.to_csv('D:\Projects\BIG_DATA_TWITTER_ANALYSIS\Dataset\CuratedData\CommunityDetection\pagerank.csv', index=False)