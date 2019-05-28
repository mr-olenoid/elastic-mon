import ssl
import json
import urllib3
import time
import pika
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
                value["memory_percent"] += one["_source"]["system"]["process"]["memory"]["rss"]["pct"]
                value["memory"] += one["_source"]["system"]["process"]["memory"]["rss"]["bytes"] 
                value["cpu"] += one["_source"]["system"]["process"]["cpu"]["total"]["pct"]
                value["count"] += 1
                exists = True
                break
        if not exists:
            service = {}
            service["name"] = one["_source"]["system"]["process"]["name"]
            service["memory_percent"] = one["_source"]["system"]["process"]["memory"]["rss"]["pct"]
            service["memory"] = one["_source"]["system"]["process"]["memory"]["rss"]["bytes"]
            service["cpu"] = one["_source"]["system"]["process"]["cpu"]["total"]["pct"]
            service["count"] = 1
            services.append(service)
    for value in services:
        value["memory"] = value["memory"] / value["count"] / 1048576
        value["memory_percent"] = value["memory_percent"] / value["count"]
        value["cpu"] = value["cpu"] / value["count"]
    services.sort(key =lambda k: k[sort_by], reverse=True)
    return services

def msg_make(serverName, services, server_name, avg_memory_used, origin, severity):
    #return json.dumps({'name': serverName, 'message': services, 'origin': origin, 'severity': severity})
    table = " <b> High memory usage: %s used memory %.2f%%</b> " % (server_name, avg_memory_used * 100)
    for service in services:
        table += "\n <i> %s used </i><b>%.2f%%</b> <i>total of </i><b>%.2fmb</b>" % (service["name"], service["memory_percent"] * 100, service["memory"])

    return json.dumps({'name': serverName, 'message': table, 'origin': origin, 'severity': severity, 'format': 'HTML'})

time_before = time.time()
#rabbit
credentials = pika.PlainCredentials(config["rabbit"]["user"], config["rabbit"]["password"])
connection = pika.BlockingConnection(pika.ConnectionParameters(config["rabbit"]["server"], config["rabbit"]["port"], '/', credentials))#add to config
channel = connection.channel()
channel.exchange_declare(exchange='alarms', exchange_type='fanout')

#check free memory sate
for srv in servers:
    res = es.search(index="metricbeat*", body=elastic_helper.get_memory_query(srv))
    avg_memory_used = get_avg(res["hits"]["hits"], res["hits"]["total"]["value"], ["_source", "system", "memory", "actual", "used", "pct"])
    if avg_memory_used > 0.70:
        res = es.search(index="metricbeat*", body=elastic_helper.get_processes_query(srv))
        services = group_services(res["hits"]["hits"], "memory")
        channel.basic_publish(exchange='alarms', 
                                routing_key='', 
                                body=msg_make("10.20.6.38", services, srv, avg_memory_used, "hardwareServers", "warning"))
        print(srv)

for srv in servers:
    res = es.search(index="metricbeat*", body=elastic_helper.get_cpu_query(srv))
    avg_cpu_idle = get_avg(res["hits"]["hits"], 
        res["hits"]["total"]["value"], 
        ["_source","system","cpu","idle","pct"])/res["hits"]["hits"][0]["_source"]["system"]["cpu"]["cores"]
    if avg_cpu_idle < 0.20:
        res = es.search(index="metricbeat*", body=elastic_helper.get_processes_query(srv))
        print(group_services(res["hits"]["hits"], "cpu"))
        #print (srv + ": " + str(get_avg(res["hits"]["hits"], res["hits"]["total"]["value"], ["_source","system","cpu","idle","pct"])/res["hits"]["hits"][0]["_source"]["system"]["cpu"]["cores"]))


print("time took: " + str(time.time() - time_before))
time_before = time.time()
