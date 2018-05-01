
import requests 
from pprint import pprint 
import arrow 
from elasticsearch import Elasticsearch 
from elasticsearch.helpers import bulk as es_bulk
import logging
import json
import uuid
import schedule
import os
import time
import yaml
from multiprocessing.pool import ThreadPool

logging.basicConfig(format="%(asctime)s - %(name)s - [ %(levelname)s ] - [%(filename)s:%(lineno)s - %(funcName)s() ] - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

SHOME = os.path.abspath(os.path.join(os.path.dirname(__file__)))

def load_config(path=os.path.join(SHOME, 'config.yml')):
    with open(path, 'r') as f:
        try:
            doc = yaml.load(f)
            return doc 
        except Exception as e:
            logger.exception('Unable to open yaml config at: {}, reason: {}'.format(path, e))

config = load_config()

es = Elasticsearch(config['elasticsearch']['hosts'], **config['elasticsearch'].get('args', {}))

ES_INDEX = config.get('es_index', 'rabbitmon') 

_default_blacklist_fields = [
    ['backing_queue_status', 'delta'],
    [ 'backing_queue_status', 'target_ram_count'],
    ['listeners', 'socket_opts', 'linger']
]

BLACKLIST_FIELDS = config.get('blacklist_fields', {})

def delete_keys_from_dict(dict_del, the_keys):
    """
    Delete the keys present in the lst_keys from the dictionary.
    Loops recursively over nested dictionaries.
    """
    # make sure the_keys is a set to get O(1) lookups
    if type(the_keys) is not set:
        the_keys = set(the_keys)
    for k,v in dict_del.items():
        if k in the_keys:
            del dict_del[k]
        if isinstance(v, dict):
            delete_keys_from_dict(v, the_keys)
    return dict_del

def get_es_index():
    return '{}-{}'.format(ES_INDEX, arrow.utcnow().format('YYYY.MM.DD'))

class RabbitMon(object):
    def __init__(self, rabbit_connection):
        rc = rabbit_connection
        self.conn_name = rc['name']
        self.base_host = rc['host']
        self.base_headers = { 'content-type': 'application/json' }
        self.auth_username = rc['username']
        self.auth_password = rc['password'] 
        self.basic_auth_tuple = ( self.auth_username, self.auth_password )

    def get_data(self, endpoint):
        try:
            response = requests.get('{base}{endpoint}'.format(base=self.base_host, endpoint=endpoint), headers=self.base_headers, auth=self.basic_auth_tuple)
            if response.status_code in range(200,299):
                return response.json()
            else:
                logger.error("Unable to request endpoint: %s, connection: %s, response: %s, status_code: %s" % (endpoint, self.conn_name, response, response.status_code))
        except Exception as e:
            logger.exception('Unable to request endpoint: %s for connection: %s, reason: %s' % (endpoint, self.conn_name, e))
    
    def clusterOverview(self):
        overview = self.get_data('/api/overview')

        for fields in BLACKLIST_FIELDS.get('clusterOverview', []):
            overview = delete_keys_from_dict(overview, fields)

        overview.update({ '@timestamp': arrow.utcnow().format('YYYY-MM-DDTHH:mm:ssZ'), 'rabbit_connection': self.conn_name })
        es.index(index=get_es_index(), body=overview, doc_type='cluster-overview')
        logger.info('All done with clusterOverview on connection: %s' % (self.conn_name))

    def nodeStats(self):
        nodes = self.get_data('/api/nodes')
        if len(nodes) > 0:
            for node in nodes:
                for fields in BLACKLIST_FIELDS.get('nodeStats', []):
                    node = delete_keys_from_dict(node, fields)

                node.update({ '@timestamp': arrow.utcnow().format('YYYY-MM-DDTHH:mm:ssZ'), 'rabbit_connection': self.conn_name })
                es.index(index=get_es_index(), body=node, doc_type='node-stats') 
        logger.info('All done with nodeStats on connection: %s' % (self.conn_name))

    def queueStats(self):
        queues = self.get_data('/api/queues')
        if queues is not None: 
            items = []
            for queue in queues:
                for fields in BLACKLIST_FIELDS.get('queueStats', []):
                    delete_keys_from_dict(queue, fields)

                es_stuff = {
                    '@timestamp': arrow.utcnow().format('YYYY-MM-DDTHH:mm:ssZ'),
                    'rabbit_connection': self.conn_name,
                    '_index': get_es_index(),
                    '_type': 'queue-stats',
                }
                es_stuff.update(queue)
                items.append(es_stuff)
            indexit = es_bulk(es, items) 
        logger.info("All done with queueStats on connection: %s, items_inserted: %s, errors: %s" % (self.conn_name, indexit[0], indexit[1]))

def worker(rabbit_connection):
    rm = RabbitMon(rabbit_connection)
    rm.clusterOverview()
    rm.queueStats()
    rm.nodeStats()


def main():
    pool = ThreadPool(processes=3)
    pool = ThreadPool()
    pool.map(worker, config['rabbit_connections'])
    pool.close()
    pool.join()

if __name__ == '__main__':
    while True:
        main()
        time.sleep(config.get('scrape_interval', 20))