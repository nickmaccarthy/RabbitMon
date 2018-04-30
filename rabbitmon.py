
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

rabbit_conection = {}

ES_INDEX = config.get('es_index', 'rabbitmon') 

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
            return response.json() 
        except Exception as e:
            logger.exception('Unable to request endpoint: %s for connection: %s, reason: %s' % (endpoint, self.conn_name, e))
    
    def clusterOverview(self):
        overview = self.get_data('/api/overview')

        overview.update( { '@timestamp': arrow.utcnow().format('YYYY-MM-DDTHH:mm:ssZ'), 'rabbit_connection': self.conn_name })
        es.index(index=get_es_index(), body=overview, doc_type='cluster-overview')
        logger.info('All done with clusterOverview on connection: %s' % self.conn_name)

    def queueStats(self):
        queues = self.get_data('/api/queues')
        if queues is not None: 
            items = []
            for queue in queues:
                del queue['backing_queue_status']['delta']
                del queue['backing_queue_status']['target_ram_count']

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