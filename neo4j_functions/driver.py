from neo4j import GraphDatabase
import pandas as pd

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
            query= ("MATCH (a:%s {%s: $idnum}) "
                    "RETURN %s " % (class_type, id_field, cols_query))
            print('Query: \n', query)
            info = session.run(query, idnum = id_val)
        if info is None:
            print('No Information Found')
            return pd.DataFrame(columns = info_cols)
        for item in info.single():
            l.append(item)
            print(item)
        df = pd.DataFrame([l], columns = info_cols)
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