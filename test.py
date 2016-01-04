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
    try:
        mapping_dict['settings']['number_of_shards'] = index_dict['settings']['number_of_shards']
        mapping_dict['settings']['number_of_replicas'] = index_dict['settings']['number_of_replicas']
    except KeyError:
        print 'Using the default number_of_shards and number_of_replicas'

    time_field_list = []
    for field_name in index_dict['fields']:
        if 'time' in field_name.lower():
            mapping_dict['mappings']['leet']['properties'][field_name] = date_mapping
            time_field_list.append(field_name)

    response = requests.put(host_url + index_name, data=json.dumps(mapping_dict))
    if 'error' in response.json().keys():
        print 'Index [' + index_name + '] already exists'

    return time_field_list


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
    time_field_list = []
    data_row = dict()
    title = raw_input('Title: ').lower().strip()
    search_query = 'Title: "query"'.replace('query', title)
    matches = es.search(index=index_name, q=search_query, size=1000)
    hits = matches['hits']['hits']
    print len(hits)
    for hit in hits:
        print hit['_source']

    new_record = True
    if len(hits) >= 1:
        choice = raw_input('Same record found, use the same record for logging? Y/N: ')
        if choice.lower() != 'n':
            data_row = hits[0]['_source']
            print data_row.keys()
            new_record = False

    if new_record:
        for field_name in index_dict['fields']:
            if 'time' in field_name.lower():
                time_field_list.append(field_name)
            else:
                if 'Difficulty' == field_name:
                    log_difficulty(data_row)
                else:
                    raw_data = raw_input(field_name + ': ')
                    data_row[field_name] = raw_data.lower().strip()

    if 1 == len(time_field_list):
        data_row[time_field_list[0]] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    else:
        sys.exit('Fun error')
    es.index(index_name, 'leet', data_row)
    time.sleep(1)

if __name__ == '__main__':
    index_json_file = 'index.json'
    start_index(index_json_file)