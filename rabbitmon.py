import requests 
from pprint import pprint 
import arrow 
from elasticsearch import Elasticsearch 
import logging
import json
import uuid
import schedule
import os
import time
import yaml

logging.basicConfig(format="%(asctime)s - %(name)s - [ %(levelname)s ] - %(message)s", level=logging.INFO)
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

es = Elasticsearch(config['elasticsearch']['hosts'], **config['elasticsearch']['args'])

def get_data(endpoint):
    try:
        return requests.get( "{base}{endpoint}".format(base=config['rabbit_host'], endpoint=endpoint), headers={ 'content-type': 'application/json' }, auth=(config['rabbit_username'], config['rabbit_password']))
    except Exception as e:
        print("Unable to query endpoint {}, reason: {}".format( endpoint, e))

def clusterOverview():
    index_name = 'rabbitmon-{}'.format(arrow.utcnow().format('YYYY.MM.DD'))
    overview = get_data('/api/overview') 
    overview = overview.json() 
    overview.update( { '@timestamp': arrow.utcnow().format('YYYY-MM-DDTHH:mm:ssZ') })
    es.index(index=index_name, body=overview, doc_type='cluster-overview')

def queueStats():
    queues = get_data('/api/queues')
    if queues is not None: 
        for queue in queues.json():
            index_name = 'rabbitmon-{}'.format(arrow.utcnow().format('YYYY.MM.DD'))
            es_stuff = {
                '@timestamp': arrow.utcnow().format('YYYY-MM-DDTHH:mm:ssZ')
            }
            queue.update(es_stuff)
            del queue['backing_queue_status']['delta']
            es.index(index=index_name, body=queue, doc_type='queue-stats')
            #logger.info("All good with queue: {}".format(queue['name']) )
    logger.info("All done with queueStats")

if __name__ == "__main__":
    schedule.every(10).seconds.do(queueStats)
    schedule.every(10).seconds.do(clusterOverview)
    while True:
        schedule.run_pending()
        time.sleep(0.5)