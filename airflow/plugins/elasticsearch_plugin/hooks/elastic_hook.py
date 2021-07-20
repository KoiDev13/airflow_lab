from airflow.hooks.base import BaseHook # This class used by all Hooks

from elasticsearch import Elasticsearch

class ElasticHook(BaseHook):

    def __init__(self, conn_id='elasticsearch_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = self.get_connection(conn_id) #When start th the ES, you need to fetch it
        #Attribute of ES
        conn_config = {}
        hosts = []

        #ElasticSearch attribute
        if conn.host:
            hosts = conn.host.split(',') #If there are multiple hosts, we can create list of hosts by ,
        if conn.port:
            conn_config['port'] = int(conn.port) #Make sure that the port is a number, not a string
        if conn.login:
            conn_config['http_auth'] = (conn.login, conn.password) #That's not require
        
        #Create Elastic Search object
        self.es = Elasticsearch(hosts, **conn_config)
        self.index = conn.schema #You can define your index where you gona store the data
    
    #Get some information about ES instance
    def info(self): 
        return self.es.info()
    
    #Define an index
    def set_index(self, index):
        self.index = index
    
    #Add document of data into index
    def add_doc(self, index, doc_type, doc):
        self.set_index(index)
        res = self.es.index(index=index, doc_type=doc_type, body=doc)
        return res