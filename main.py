import ssl
import json
import urllib3
import time
import elastic_helper
from elasticsearch import Elasticsearch
from elasticsearch.connection import create_ssl_context
from collections import defaultdict

ssl_context = create_ssl_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
#"kie00dfs-srv02"
with open('config.json', 'r') as f:
        config = json.load(f)

servers= config["server_list"]

es = Elasticsearch(config["elastic"]["server"], ssl_context=ssl_context, http_auth=(config["elastic"]["user"], config["elastic"]["password"]))

def nested_get(input_dict, nested_key):
    internal_dict_value = input_dict
    for k in nested_key:
        internal_dict_value = internal_dict_value.get(k, None)
        if internal_dict_value is None:
            return None
    return internal_dict_value

def get_avg(data, count, nested_key):
    sum = 0.0
    for i in range(count):
        sum += nested_get(data[i], nested_key)
    return sum / count

def group_services(data, sort_by):
    services = []
    for one in data:
        exists = False
        for value in services:
            if one["_source"]["system"]["process"]["name"] == value["name"]:
                value["memory"] += one["_source"]["system"]["process"]["memory"]["rss"]["pct"]
                value["cpu"] += one["_source"]["system"]["process"]["cpu"]["total"]["pct"]
                value["count"] += 1
                exists = True
                break
        if not exists:
            service = {}
            service["name"] = one["_source"]["system"]["process"]["name"]
            service["memory"] = one["_source"]["system"]["process"]["memory"]["rss"]["pct"]
            service["cpu"] = one["_source"]["system"]["process"]["cpu"]["total"]["pct"]
            service["count"] = 1
            services.append(service)
    for value in services:
        value["memory"] = value["memory"] / value["count"]
        value["cpu"] = value["cpu"] / value["count"]
    services.sort(key =lambda k: k[sort_by], reverse=True)
    return services

time_before = time.time()

#check free memory sate
for srv in servers:
    res = es.search(index="metricbeat*", body=elastic_helper.get_memory_query(srv))
    avg_memory_used = get_avg(res["hits"]["hits"], res["hits"]["total"]["value"], ["_source","system","memory","actual", "used","pct"])
    if avg_memory_used > 0.70:
        print(srv + " " + str(avg_memory_used))
        print(res["hits"]["total"])
        res = es.search(index="metricbeat*", body=elastic_helper.get_processes_query(srv))
        #print(res["hits"]["hits"][0]["_source"]["system"]["process"]["name"])
        #print(res["hits"]["hits"][0]["_source"]["system"]["process"]["memory"]["rss"]["pct"])
        print(group_services(res["hits"]["hits"], "memory"))
    #print(res["hits"]["hits"][0]["_source"]["system"]["memory"]["actual"]["used"]["pct"])

#for srv in servers:
#    res = es.search(index="metricbeat*", body=elastic_helper.get_cpu_query(srv))
#    print (srv + ": " + str(get_avg(res["hits"]["hits"], res["hits"]["total"]["value"], ["_source","system","cpu","idle","pct"])/res["hits"]["hits"][0]["_source"]["system"]["cpu"]["cores"]))

print("time took: " + str(time.time() - time_before))
time_before = time.time()
