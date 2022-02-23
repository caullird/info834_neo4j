# import the neo4j driver for Python
from neo4j import GraphDatabase
import configparser

config = configparser.ConfigParser()
config.read('config/config.ini')

# Connect to the neo4j database server
graphDB_Driver  = GraphDatabase.driver(config['NEO4J']['uri'], auth=(config['NEO4J']['userName'], config['NEO4J']['password']))

# CQL to delete all nodes and relationships
cqlDeletePaths = "MATCH (x)-[r]->(y) delete r"
cqlDeleteNodes = "MATCH (x) delete x"

# CQL to create a graph containing some of the Ivy League universities
cqlCreate = """
CREATE (cornell:university { name: "Cornell University"}),

(yale:university { name: "Yale University"}),
(princeton:university { name: "Princeton University"}),
(harvard:university { name: "Harvard University"}),

(cornell)-[:connects_in {miles: 259}]->(yale),
(cornell)-[:connects_in {miles: 210}]->(princeton),
(cornell)-[:connects_in {miles: 327}]->(harvard),

(yale)-[:connects_in {miles: 259}]->(cornell),
(yale)-[:connects_in {miles: 133}]->(princeton),
(yale)-[:connects_in {miles: 133}]->(harvard),

(harvard)-[:connects_in {miles: 327}]->(cornell),
(harvard)-[:connects_in {miles: 133}]->(yale),
(harvard)-[:connects_in {miles: 260}]->(princeton),

(princeton)-[:connects_in {miles: 210}]->(cornell),
(princeton)-[:connects_in {miles: 133}]->(yale),
(princeton)-[:connects_in {miles: 260}]->(harvard)"""

# Execute the CQL query
with graphDB_Driver.session() as graphDB_Session:

    # Delete paths and nodes
    graphDB_Session.run(cqlDeletePaths)
    graphDB_Session.run(cqlDeleteNodes)

    # Create nodes
    graphDB_Session.run(cqlCreate)

    #_____________________________________________________________
    # Question 1 : Récupération de l'ensemble des noeuds du graphe

    print("Question 1 : Récupérer l’ensemble des Node d’un graphe") 

    # Nous souhaitons récupérer l'ensemble des université
    # donc sans l'ensemble des élements sans contrainte

    query = "MATCH (x:university) RETURN x"
    nodes = graphDB_Session.run(query)
    for node in nodes:
        print(node)
    print("\n")

    #_____________________________________________________________
    # Question 2 : Récupération d'un node en particulier

    print("Question 2: Récupérer un Node particulier")

    # Nous souhaitons récupérer une ligne particulière d'une table (contrainte WHERE)
    # Dans cette exemple nous réalisons la requête avec l'université "Yale University"
    
    query = "MATCH (x:university {name:'Yale University'}) RETURN x"
    nodes = graphDB_Session.run(query)
    for node in nodes:
        print(node)
    print("\n")

    #_____________________________________________________________
    # Question 3 : Récupération de l'ensemble des liens du graphe
    
    print("Question 3: Récupérer l’ensemble des Relationship (ou Path) d’un graphe")
    query = "MATCH (x:university)-[r]->(y:university) RETURN x.name,y.name,r.miles"
    nodes = graphDB_Session.run(query)
    for node in nodes:
        print(node)
    print("\n")


    #_____________________________________________________________
    # Question 4 : Récupération d'un lien spécifique

    print("Question 4: Récupérer un Relationship (ou Path) spécifique")

    # Pour l'exemple, nous souhaitons récupérer le lien entre 'Yale University'
    # et 'Cornell University'

    query = "MATCH (x:university {name:'Yale University'})-[r]->(y:university {name:'Cornell University'}) RETURN x.name,y.name,r.miles"
    nodes = graphDB_Session.run(query)
    for node in nodes:
        print(node)
    print("\n")

    #_____________________________________________________________
    # Question 5 : Création d'un nouveau noeud

    print("Question 5: Créer un Node (nous considérons que le graphe existe déjà sur Neo4j Desktop)")
    query = "CREATE (polytech:university { name: 'Polytech University'}) RETURN polytech"
    nodes = graphDB_Session.run(query)
    for node in nodes:
        print(node)
    print("\n")

    #_____________________________________________________________
    # Question 6 : Création d'un nouveau Relationship

    print("Question 6: Créer un Relationship (en créant les Node correspondant ou pas)")    
    query = """
    MATCH (x:university {name:'Polytech University'})
    CREATE (y:university { name: 'Savoie University'})
    CREATE (x)-[r:connects_in {miles: 42}]->(y) 
    RETURN x.name,y.name,r.miles"""
    nodes = graphDB_Session.run(query)
    for node in nodes:
        print(node)
    print("\n")
