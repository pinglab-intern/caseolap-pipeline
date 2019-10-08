from neo4j import GraphDatabase
import pandas as pd
from typing import List

class driver(object):
    
    
    """initialize driver to communicate with local host"""
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.parents = {}
        self.children = {}
        
    
    """View info about single node in as pandas df"""
    def get_node_info(self, id_field, id_val, class_type, info_cols):
        l = []
        with self.driver.session() as session:
            cols_query = 'a.' + ', a.'.join(info_cols)
            query = ("MATCH (a:%s {%s: $idnum}) "
                    "RETURN %s " % (class_type, id_field, cols_query))
            print('Query: \n', query)
            info = session.run(query, idnum=id_val)
        if info is None:
            print('No Information Found')
            return pd.DataFrame(columns=info_cols)
        for item in info.single():
            l.append(item)
            print(item)
        df = pd.DataFrame([l], columns=info_cols)
        return df

    def get_n_relations(
        self,
        class_1: str,
        id_1: str,
        id_class: str,
        class_2: str,
        info_cols: List[str],
        edge_type: str,
        n: int,
        verbose: bool = False,
        where_clause: str = '',
    ) -> pd.DataFrame:
        """ Get N (int) nodes of a certain class related to a specific nodes as a pandas DataFrame
        
        Arguments:
            class_1 {str} -- class of specified node 
            id_1 {str} -- id of specified node, referred to in cypher as 'a'
            id_class {str} -- class of specified node
            class_2 {str} -- class of nodes to search for, refered to in cypher as 'b'
            info_cols {List[str]} -- columns of data to return ex: ['a.displayName', 'b.displayName']
            edge_type {str} -- type of connection. For 1-3 length connections of any type, edge_type would be "*1..3"
            n {int} -- number of results to return
        
        Keyword Arguments:
            verbose {bool} -- Whether to print query sequence and other details (default: {False})
            where_clause {str} -- limit search results using a cypher WHERE statement (default: {''})
        
        Returns:
            pd.DataFrame -- Dataframe of results with columns indicated in info_cols
        """
        with self.driver.session() as session:
            cols_query = ', '.join(info_cols)

            match_query = (
                "MATCH p=(a:%s {%s: %s})-[%s]-(b:%s)"
                % (class_1, id_class, id_1, edge_type, class_2)
                )

            # Add leading space to where clause
            if where_clause != '' and where_clause[0] != ' ':
                where_clause = ' ' + where_clause

            order_section = (
                " ORDER BY edgeLength"
                )
            return_section = (
                " RETURN DISTINCT %s, length(p) AS edgeLength" % cols_query
                )
            limit_section = (
                " LIMIT %s" % str(n)
            )
            query = (
                match_query +
                where_clause +
                return_section +
                order_section +
                limit_section
            )
            if verbose:
                print('Query: \n', query)
            info = session.run(query)
            if info is None:
                if verbose:
                    print('No Information Found')
                return pd.DataFrame(columns=info_cols)
        df = pd.DataFrame(columns=info_cols)
        for item in info:
            item_df = pd.DataFrame([item.values()], columns=info_cols + ['edgeLength'])
            df = pd.concat([df, item_df])
            
        return df


    """View info about n-node in as pandas df"""
    def get_n_nodes_info(self, class_type, info_cols, n, id_field=None, id_val=None):
        l = []
        with self.driver.session() as session:
            cols_query = 'a.' + ', a.'.join(info_cols)
            if not (id_field and id_val):
                query= ("MATCH (a:%s) "
                        "RETURN %s "
                        "LIMIT %s" % (class_type, cols_query, str(n)))
            else:
                query= ("MATCH (a:%s {%s: $idnum}) "
                        "RETURN %s "
                        "LIMIT %s" % (class_type, id_field, cols_query, str(n)))
            print('Query: \n', query)
            info = session.run(query, idnum = id_val)
            if info is None:
                print('No Information Found')
                return pd.DataFrame(columns = info_cols)
        df = pd.DataFrame(columns=info_cols)
        for item in info:
            item_df = pd.DataFrame([item.values()], columns = info_cols)
            df = pd.concat([df, item_df])
            
        return df
    
    """Search for a value in an array type field as pandas df"""
    def search_item_in_array(self, class_type, info_cols, item, array_field, verbose=False):
        l = []
        with self.driver.session() as session:
            cols_query = 'a.' + ', a.'.join(info_cols)
            query= ("MATCH (a:%s) "
                    "WHERE toLower(%s) IN [x in a.%s | toLower(x)] "
                    "RETURN %s " % (class_type, item, array_field, cols_query))
            if verbose:
                print('Query: \n', query)
            info = session.run(query)
            if info is None:
                return pd.DataFrame(columns = info_cols)
        df = pd.DataFrame(columns=info_cols)
        for item in info:
            item_df = pd.DataFrame([item.values()], columns = info_cols)
            df = pd.concat([df, item_df])
            
        return df

def get_n_relations(
    self,
    class_1: str,
    id_1: str,
    id_class: str,
    class_2: str,
    info_cols: List[str],
    edge_type: str,
    n: int,
    verbose: bool = False,
    where_clause: str = '',
    ) -> pd.DataFrame:
