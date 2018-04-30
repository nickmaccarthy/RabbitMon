RabbitMon
================

Scrapes the RabbitMQ API, formats the JSON and sends it back to an Elasticsearch cluster for further study


# Requirements
* python 2.7 or python 3.4+
* pip 
* virtualenv 

# Setup
1. Install the requirements, example using virtualenv 
```
virtualenv env && source env/bin/activate && pip install -r requirements.txt 
```
2. Run it and preferably deamonize the process - see `rabbitmon.supervisor.d.conf` for an example on using this with [Supervisord](http://supervisord.org/)

