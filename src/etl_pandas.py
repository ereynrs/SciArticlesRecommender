import pandas as pd
import re

from neo4j import GraphDatabase

class Neo4jConnection:
    """ Provides the methods to run queries against a given Neo4j db.
    
    """

    # credentials
    URI = "bolt://localhost:7687"
    USER = "neo4j"
    PWD = "12345678"
    
    def __init__(self):
        """ Default contructor.
        
        """
        
        self.__uri = Neo4jConnection.URI
        self.__user = Neo4jConnection.USER
        self.__pwd = Neo4jConnection.PWD
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        """ Close the connection to the db.
        

        """
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, parameters=None, db=None):
        """ Runs a query 
        
        Args.
            query (string) - Cypher query.
            parameters (object) -  Parameters of the query.
            db (object) - db to run the query.
        
        Returns.
            Query response.
        """

        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response


def get_authors():
    """ Retrieve and process data about authors serialized in CSV file.
    
    """

    authors = pd.read_csv('data/authors.csv')
    
    # streamline column names
    authors.columns = ['author_id', 'full_name', 'h_index', 'research_sector']
    
    # author id and research sector id casted to strings
    authors.author_id = authors.author_id.astype(str)
    authors.research_sector = authors.research_sector.astype(str)
    
    return authors


def get_topics():
    """ Retrieve and process data about topics serialized in CSV file.
    Only topics mentioned in the published and incoming publications are loaded.
    
    """

    topics = pd.read_csv('data/topics.csv')
    
    # streamline column names
    topics.columns = ['topic_id', 'name']
    
    # topic id casted to string
    topics.topic_id = topics.topic_id.astype(str)

    # fill null value on topic with topic_id = 164917456
    topics.name.fillna('Not Available', inplace=True)

    # get publications data
    publications = get_publications()
    incoming_publications = get_incoming_publications()

    # creates a set of the publications topics (both published and incoming ones)
    pub_topics = set(pd.concat([publications, incoming_publications]).topic_list.explode())

    # only get details of topics mentioned on publications
    pub_topic_details = topics[topics.topic_id.apply(lambda x: x in pub_topics)]

    return pub_topic_details


def get_publications():
    """ Retrieve and process data about publications serialized in CSV file. 
    
    """

    publications = pd.read_csv('data/publications.csv')
    
    # streamline column names
    publications.columns = ['publication_id', 'author_list', 'topic_list', 'publication_year', 'doi']

    # publication id casted to string
    publications.publication_id = publications.publication_id.astype(str)

    # author id extracted as list of strings
    publications.author_list = publications.author_list.apply(lambda x: re.findall(r'\d+', x))

    # topic id extracted as list of strings
    publications.topic_list = publications.topic_list.apply(lambda x: re.findall(r'\d+', x))
    
    return publications


def get_incoming_publications():
    """ Retrieve and process data about incoming publications serialized in CSV file.
    
    """

    incoming_publications = pd.read_csv('data/incoming_publications.csv')
    
    # streamline column names
    incoming_publications.columns = ['publication_id', 'author_list', 'topic_list', 'publication_year', 'doi']

    # publication id casted to string
    incoming_publications.publication_id = incoming_publications.publication_id.astype(str)

    # author id extracted as list of strings
    incoming_publications.author_list = incoming_publications.author_list.apply(lambda x: re.findall(r'\d+', x))

    # topic id extracted as list of strings
    incoming_publications.topic_list = incoming_publications.topic_list.apply(lambda x: re.findall(r'\d+', x))    
    
    return incoming_publications


def cleaned_authors_publications():
    """ Delete authors with duplicated names and replace them in the publications authors list.
    For authors with duplicated names, the ones with h-index is kept.
    
    """
    
    # get pre-processed authors, publications, and incoming_publications
    authors = get_authors()
    publications = get_publications()
    incoming_publications = get_incoming_publications()

    # duplicated authors
    duplicated = authors[authors.duplicated(['full_name'], keep=False)]

    # for each duplicated author: 
    # the author data record with highest h_index will be kept
    # in the publication and the incoming_publications data, 
    # the author_id with the remaining h_index will be replaced by the other one
    for name in set(duplicated.full_name):
    
        # index of the row of a duplicated author with highest h_index
        to_keep = authors.query(f"full_name == '{name}'").h_index.idxmax()
        id_to_keep = authors.loc[to_keep].author_id

        # index of the row of a duplicated author with lowest h_index
        to_remove = authors.query(f"full_name == '{name}'").iloc[::-1].h_index.idxmin()
        id_to_remove = authors.loc[to_remove].author_id

        # remove authors record with the lowest h_index
        authors.drop(to_remove, inplace = True)

        # in publication data, author_id with the lowest h_inde is replaced 
        publications.author_list = publications.author_list.apply(lambda x: 
                                                                    list(map(lambda y:
                                                                            y.replace(id_to_remove, id_to_keep), x)))

        # in incoming_publication data, author_id with the lowest h_inde is replaced 
        incoming_publications.author_list = incoming_publications.author_list.apply(lambda x: 
                                                                                            list(map(lambda y:
                                                                                                    y.replace(id_to_remove, id_to_keep), x)))

    return authors, publications, incoming_publications


def load_authors(conn, authors):
    """ Run the query to load the authors in the graph db.
    
    Args.
        conn (connection object) - Connection to run the query.
        authors (Pandas DataFrame) - Authors to load. 
    
    Return.
        Number of authors loaded.
    """

    query = '''
            UNWIND $rows AS row
            MERGE (t: Author 
                {
                    author_id: row.author_id, 
                    full_name: row.full_name,
                    h_index: row.h_index, 
                    research_sector: row.research_sector
                }
            )
            
            RETURN count(*) as total
            '''
    return conn.query(query, parameters = {'rows':authors.to_dict('records')})


def load_topics(conn, topics):
    """ Run the query to load the topics in the graph db.
    
    Args.
        conn (connection object) - Connection to run the query.
        topics (Pandas DataFrame) - Topics to load. 
    
    Return.
        Number of topics loaded.
    """

    query = '''
            UNWIND $rows AS row
            MERGE (t: Topic 
                {
                    topic_id: row.topic_id,
                    name: row.name
                }
            )
            
            RETURN count(*) as total
            '''
    return conn.query(query, parameters = {'rows':topics.to_dict('records')})
    

def load_publications(conn, publications):
    """ Run the query to load the publications in the graph db.
    
    Args.
        conn (connection object) - Connection to run the query.
        publications (Pandas DataFrame) - Publications to load. 
    
    Return.
        Number of publications loaded.
    """

    query = '''
            UNWIND $rows AS row
            MERGE (p: Publication 
                {
                    publication_id: row.publication_id, 
                    publication_year: row.publication_year, 
                    doi: row.doi, 
                    status: "published"
                }
            )

            WITH distinct row, p
            UNWIND row.author_list AS a_id
            MATCH (a: Author 
                {
                    author_id: a_id
                }
            )
            MERGE (a)-[:WRITES]->(p)

            WITH row, p
            UNWIND row.topic_list AS t_id
            MATCH (t: Topic 
                {
                    topic_id: t_id
                }
            )
            MERGE (p)-[:IS_ABOUT]->(t)
            
            RETURN count(distinct p) as total
            '''
    return conn.query(query, parameters = {'rows':publications.to_dict('records')})


def load_incoming_publications(conn, incoming_publications):
    """ Run the query to load the incoming publications in the graph db.
    
    Args.
        conn (connection object) - Connection to run the query.
        incoming_publications (Pandas DataFrame) - Incoming publications to load. 
    
    Return.
        Number of incoming publications loaded.
    """

    query = '''
            UNWIND $rows AS row
            MERGE (p: Publication 
                {
                    publication_id: row.publication_id, 
                    publication_year: row.publication_year, 
                    doi: row.doi, 
                    status: "published"
                }
            )

            WITH distinct row, p
            UNWIND row.author_list AS a_id
            MATCH (a: Author 
                {
                    author_id: a_id
                }
            )
            MERGE (a)-[:WRITES]->(p)

            WITH row, p
            UNWIND row.topic_list AS t_id
            MATCH (t: Topic 
                {
                    topic_id: t_id
                }
            )
            MERGE (p)-[:IS_ABOUT]->(t)
            
            RETURN count(distinct p) as total
            '''
    return conn.query(query, parameters = {'rows':incoming_publications.to_dict('records')})


if __name__ == "__main__":

    # creates the connection to the graph db.
    conn = Neo4jConnection()

    # get cleaned authors, publications, and incoming publications
    authors, publications, incoming_publications = cleaned_authors_publications() 

    # load authors
    print("Loading Authors ...")
    print(load_authors(conn, authors))

    # load topics
    print("Loading Topics ...")
    topics = get_topics()
    print(load_topics(conn, topics))

    # load publications
    print("Loading Publications ...")
    print(load_publications(conn, publications))

    # load incoming publications
    print("Loading Incoming Publications ...")
    print(load_incoming_publications(conn, incoming_publications))