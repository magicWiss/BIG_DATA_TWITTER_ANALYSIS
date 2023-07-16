from pyspark.sql import SparkSession
from graphframes import GraphFrame
from neo4j import GraphDatabase
import json
import argparse

NEO4J_DATA_FORMAT="org.neo4j.spark.DataSource"

def get_config():
    # path="file:///home/federico/BD/BIG_DATA_TWITTER_ANALYSIS/Logics/5_LoadDataNeo4J/config.json"
    # with open(path,"r")as f:
    #     data=json.load(f)
    data = {}
    data['uri'] = "bolt://localhost:7687"
    data['username'] = "neo4j"
    data['password'] = "password"


    return data

# Define a function to write the graph to Neo4j
def write_to_neo4j(graph):
    conf = get_config()
    driver = GraphDatabase.driver(conf['uri'], auth=(conf['username'], conf['password']))
    with driver.session() as session:
        result = session.run("""
            UNWIND $nodes AS node
            MERGE (n:Author {name: node.id})
        """, {"nodes": graph.vertices.toPandas().to_dict('records')})

        result = session.run("""
            UNWIND $rels AS rel
            MATCH (src:Author {name: rel.src})
            MATCH (dst:Author {name: rel.dst})
            MERGE (src)-[:CONVERSATION]->(dst)
        """, {"rels": graph.edges.toPandas().to_dict('records')})


#create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")

args = parser.parse_args()
input_filepath = args.input_path


spark = SparkSession.builder \
    .appName("CSV to Neo4j") \
    .getOrCreate()
df = spark.read.csv(input_filepath, header=True, inferSchema=True)

# Create vertices (current_author nodes)
vertices = df.select("author_id").distinct().withColumnRenamed("author_id", "id")

# Create edges (links between authors)
edges = df.select("author_id", "conversation_author_id", "tweet_id", "conversation_id") \
    .withColumnRenamed("author_id", "src") \
    .withColumnRenamed("conversation_author_id", "dst")

# Create the graph
graph = GraphFrame(vertices, edges)

write_to_neo4j(graph)