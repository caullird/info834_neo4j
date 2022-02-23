# import the neo4j driver for Python
from neo4j import GraphDatabase
import pandas as pd
import re
import configparser

config = configparser.ConfigParser()
config.read('config/config.ini')

# Connect to the neo4j database server
graphDB_Driver  = GraphDatabase.driver(config['NEO4J']['uri'], auth=(config['NEO4J']['userName'], config['NEO4J']['password']))

# import csv in dataframe
df = pd.read_csv("data/avengers.csv")

# CQL to delete all nodes and relationships
cqlDeletePaths = "MATCH (x)-[r]->(y) delete r"
cqlDeleteNodes = "MATCH (x) delete x"

# generate cqlCreate avengers
cqlCreate = "CREATE"
for index in df.head(100).iterrows():
    cqlCreate += "(:avenger {"
    for (key, value) in index[1].items():
        cqlCreate += str(key).replace(" ", "_") + " : '" + re.sub(r'[^\w\s]', '', str(value)) + "',"
    cqlCreate = cqlCreate[:-1]
    cqlCreate += "}),"
cqlCreate = cqlCreate[:-1]

cqlCreateGender = """CREATE (male:gender { name: "Homme"}), (female:gender { name: "Femme"})"""
cqlLinkGenderMale = """match (h:avenger {gender:"male"}), (gh:gender {name:"Homme"}) CREATE (h)-[:is]->(gh)"""
cqlLinkGenderFemale = """match (f:avenger {gender:"female"}), (gf:gender {name:"Femme"}) CREATE (f)-[:is]->(gf)"""

# Execute the CQL query
with graphDB_Driver.session() as graphDB_Session:

    # Delete paths and nodes
    graphDB_Session.run(cqlDeletePaths)
    graphDB_Session.run(cqlDeleteNodes)

    # Create nodes
    graphDB_Session.run(cqlCreate)
    
    # Link gender
    graphDB_Session.run(cqlCreateGender)
    graphDB_Session.run(cqlLinkGenderMale)
    graphDB_Session.run(cqlLinkGenderFemale)

