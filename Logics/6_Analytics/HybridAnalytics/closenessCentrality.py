from graphdatascience import GraphDataScience
from neo4j import GraphDatabase, Result

uri = "bolt://localhost:7687"
user = 'neo4j'
password = 'password'

driver = GraphDatabase.driver(uri, auth=(user, password), database='neo4j')

def get_node_ids(idxs_list):
    pandas_df = driver.execute_query(
        f"MATCH (n:Author) WHERE ID(n) IN {idxs_list} RETURN ID(n) AS nodeId, n.id AS id",
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

topic = 6

G = gds.graph.get('myProjection')

gds.beta.closeness.mutate(G, mutateProperty="closeness")

res_df = gds.graph.nodeProperties.stream(G, ['closeness'])
res_df_closeness = res_df[res_df['nodeProperty'] == 'closeness'].drop(['nodeProperty'], axis=1)
df_nodeIdx_nodeId = get_node_ids(res_df_closeness['nodeId'].agg(list))
res_df_closeness['nodeId'] = res_df_closeness['nodeId'].apply(nodeidxtonodeid)
res_df_closeness.to_csv(f'D:\Projects\BIG_DATA_TWITTER_ANALYSIS\Dataset\CuratedData\HybridAnalysis\closeness{topic}.csv', index=False)