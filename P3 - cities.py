# import the neo4j driver for Python
from neo4j import GraphDatabase
import pandas as pd
import time
import re

# import csv in dataframe
df = pd.read_csv("communes-departement-region.csv")

# Database Credentials
uri             = "bolt://localhost:7687"
userName        = "neo4j"
password        = "123soleil"

# Connect to the neo4j database server
graphDB_Driver  = GraphDatabase.driver(uri, auth=(userName, password))

# CQL to delete all nodes and relationships
cqlDeletePaths = "MATCH (x)-[r]->(y) delete r"
cqlDeleteNodes = "MATCH (x) delete x"

cqlGetDepartment = "MATCH (n:city) RETURN DISTINCT n.nom_departement"
cqlGetRegion = "MATCH (n:city) RETURN DISTINCT n.nom_region"

cqlCreateDepartment = "MATCH (c:city) MERGE (d:departement { name: apoc.text.clean(c.nom_departement), nom_region: apoc.text.clean(c.nom_region) }) CREATE (c)-[:in]->(d) "
cqlCreateRegion = "MATCH (d:departement) MERGE (r:region { name: apoc.text.clean(d.nom_region)}) CREATE (d)-[:in]->(r)"
cqlCreateFrance= "MATCH (r:region) MERGE (f:france { name: 'France'}) CREATE (r)-[:in]->(f)"

cqlGephi = """
MATCH path = (:city)-[in]-(:departement)-[:in]-(:region)-[:in]-(:france)
WITH path LIMIT 39000 
WITH collect(path) as paths
CALL apoc.gephi.add('http://localhost:8080','workspace1', paths) YIELD nodes, relationships, time
RETURN nodes, relationships, time"""

# Execute the CQL query
with graphDB_Driver.session() as graphDB_Session:

    # Delete paths and nodes
    graphDB_Session.run(cqlDeletePaths)
    graphDB_Session.run(cqlDeleteNodes)

    # generate cqlCreate
    i = 0
    start = time.time()
    cqlCreate = "CREATE"
    for index in df.head(39000).iterrows():
        cqlCreate += "(:city {"
        for (key, value) in index[1].items():
            cqlCreate += str(key).replace(" ", "_") + " : '" + re.sub(r'[^\w\s]', '', str(value)) + "',"
        cqlCreate = cqlCreate[:-1]
        cqlCreate += "}),"
        i += 1
        if (i%1000 == 0): 
            # Create nodes
            cqlCreate = cqlCreate[:-1]
            graphDB_Session.run(cqlCreate)
            cqlCreate = "CREATE"
            # Log time
            print(i, "cities load and inserted in", format(time.time() - start, '.2f'), "seconds")


    # Create nodes
    if (cqlCreate != "CREATE"):
        cqlCreate = cqlCreate[:-1]
        graphDB_Session.run(cqlCreate)
    print("All data inserted in", format(time.time() - start, '.2f'), "seconds")
    
    # Create nodes
    graphDB_Session.run(cqlCreateDepartment)
    graphDB_Session.run(cqlCreateRegion)
    graphDB_Session.run(cqlCreateFrance)
    
    # Connect to gephi
    graphDB_Session.run(cqlGephi)