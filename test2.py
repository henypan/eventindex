import sys
import datetime
import requests
import json
import time

from elasticsearch import Elasticsearch
from collections import OrderedDict


def init_index(host_url, index_name, index_dict):
    mapping_dict = {"settings": {"number_of_shards": 1, "number_of_replicas": 0}, "mappings": {
        "leet": {"_all": {"enabled": False}, "dynamic_templates": [{"template_1": {
            "mapping": {"index": "not_analyzed", "type": "string"}, "match_mapping_type": "string", "match": "*"}}],
            "properties": {}}}}
    date_mapping = {"type": "date", "format": "dateOptionalTime"}
    num_mapping = {"type": "long"}
    try:
        mapping_dict['settings']['number_of_shards'] = index_dict['settings']['number_of_shards']
        mapping_dict['settings']['number_of_replicas'] = index_dict['settings']['number_of_replicas']
    except KeyError:
        print 'Using the default number_of_shards and number_of_replicas'

    for field_name in index_dict['fields']:
        if 'time' in field_name.lower():
            mapping_dict['mappings']['leet']['properties'][field_name] = date_mapping
        if 'number' in field_name.lower():
            mapping_dict['mappings']['leet']['properties'][field_name] = num_mapping

    response = requests.put(host_url + index_name, data=json.dumps(mapping_dict))
    if 'error' in response.json().keys():
        print 'Index [' + index_name + '] already exists'


def log_difficulty(data_row):
    raw_data = raw_input('Difficulty: ').strip().lower()
    if len(raw_data) >= 1:
        if raw_data[0] == 'e':
            raw_data = 'Easy'
        elif raw_data[0] == 'm':
            raw_data = 'Medium'
        elif raw_data[0] == 'h':
            raw_data = 'Hard'
    data_row['Difficulty'] = raw_data


def start_index(index_json):
    print 'Index Leet Code'
    with open(index_json) as json_file:
        index_dict = json.load(json_file, object_pairs_hook=OrderedDict)
    try:
        host_ip = index_dict['host']
        index_name = index_dict['index_name']
    except KeyError:
        sys.exit('The format of input JSON is not correct.')

    es = Elasticsearch(hosts=host_ip, timeout=120)
    host_url = 'http://' + host_ip + ':9200/'
    if not es.indices.exists(index=index_name):
        init_index(host_url, index_name, index_dict)
    data_row = dict()
    title = raw_input('Title: ').lower().strip()
    search_query = 'Title: "query"'.replace('query', title)
    matches = es.search(index=index_name, q=search_query, size=1000)
    hits = matches['hits']['hits']

    if len(hits) >= 1:
        choice = raw_input(str(len(hits)) + ' record found, use the same record for logging? Y/N: ')
        data_row = hits[0]['_source']
        if choice.lower() != 'n':
            pass
        else:
            print 'Reload method and note\n'
            for field_name in index_dict['fields']:
                if 'method' in field_name.lower() or 'note' in field_name.lower():
                    raw_data = raw_input(field_name + ': ')
                    data_row[field_name] = raw_data.lower().strip()

    else:
        data_row['Title'] = title
        for field_name in index_dict['fields']:
            if 'time' in field_name.lower() or 'title' in field_name.lower():
                pass
            else:
                if 'Difficulty' == field_name:
                    log_difficulty(data_row)
                else:
                    raw_data = raw_input(field_name + ': ')
                    data_row[field_name] = raw_data.lower().strip()

    data_row['LogTime'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    for k, v in data_row.items():
        print k, ':', v
    input = raw_input('\nIndex the data to elasticsearch? (Y)/N: ')
    if input.lower() != 'n':
        es.index(index_name, 'leet', data_row)
        print 'Data loaded successfully'
    else:
        print 'Mission aborted'
    time.sleep(1)

if __name__ == '__main__':
    index_json_file = 'index.json'
    start_index(index_json_file)