import ssl
import json
import urllib3
import time
import pika
import elastic_helper
from elasticsearch import Elasticsearch
from elasticsearch.connection import create_ssl_context
from collections import defaultdict
import db_helper

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

def msg_make(services, server_name, avg_memory_used, origin, severity):
    table = " <b> High memory usage: %s used memory %.2f%%</b> " % (server_name, avg_memory_used * 100)
    for service in services:
        table += "\n <i> %s used </i><b>%.2f%%</b> <i>total of </i><b>%.2fmb</b>" % (service["name"], service["memory_percent"] * 100, service["memory"])
    return json.dumps({'name': server_name, 'message': table, 'origin': origin, 'severity': severity, 'format': 'HTML'})

def msg_make_cpu(services, server_name, avg_cpu_used, origin, severity):
    table = " <b> High cpu usage: %s used cpu %.2f%%</b> " % (server_name, avg_cpu_used * 100)
    for service in services:
        table += "\n <i> %s used </i><b>%.2f%%</b> <i> of total cpu</i>" % (service["name"], service["cpu"] * 100)
    return json.dumps({'name': server_name, 'message': table, 'origin': origin, 'severity': severity, 'format': 'HTML'})

server_falt = []
server_high_memory = []
server_high_cpu = []

def logic(servers, config):
    es = Elasticsearch(config["elastic"]["server"], ssl_context=ssl_context, http_auth=(config["elastic"]["user"], config["elastic"]["password"]))
    credentials = pika.PlainCredentials(config["rabbit"]["user"], config["rabbit"]["password"])
    connection = pika.BlockingConnection(pika.ConnectionParameters(config["rabbit"]["server"], config["rabbit"]["port"], '/', credentials))#add to config
    channel = connection.channel()
    channel.exchange_declare(exchange='alarms', exchange_type='fanout')

    for srv in servers:
        res = es.search(index="metricbeat*", body=elastic_helper.get_general(srv[0], "memory"))
        if res["hits"]["total"]["value"] < 4:
            if srv not in server_falt:
                server_falt.append(srv)
                channel.basic_publish(exchange='alarms',
                    routing_key='',
                    body=json.dumps({'name': srv[0], 'message': "Too many lost data packets from: " + srv[0], 'origin': "windowsServers", 'severity': 'Error', 'format': 'HTML'}))
        else:
            if srv in server_falt:
                server_falt.remove(srv)
                channel.basic_publish(exchange='alarms', 
                                        routing_key='', 
                                        body=json.dumps({'name': srv[0], 'message': "Server back in shape: " + srv[0], 'origin': "windowsServers", 'severity': 'Error', 'format': 'HTML'}))

        if res["hits"]["total"]["value"] > 0:
            avg_memory_used = get_avg(res["hits"]["hits"], res["hits"]["total"]["value"], ["_source", "system", "memory", "actual", "used", "pct"])
            if avg_memory_used > 0.80:
                if srv not in server_high_memory:
                    server_high_memory.append(srv)
                    res = es.search(index="metricbeat*", body=elastic_helper.get_general(srv[0], "process"))
                    services = group_services(res["hits"]["hits"], "memory")
                    channel.basic_publish(exchange='alarms', 
                                            routing_key='', 
                                            body=msg_make(services, srv[0], avg_memory_used, "windowsServers", "warning"))
            else:
                if srv in server_high_memory:
                    server_high_memory.remove(srv)
                    channel.basic_publish(exchange='alarms', 
                                        routing_key='', 
                                        body=json.dumps({'name': srv[0], 'message': "Server is no longer low on memory: " + srv[0], 'origin': "windowsServers", 'severity': 'Error', 'format': 'HTML'}))

            res = es.search(index="metricbeat*", body=elastic_helper.get_general(srv[0], "cpu"))
            core_count = res["hits"]["hits"][0]["_source"]["system"]["cpu"]["cores"]
            avg_cpu_idle = get_avg(res["hits"]["hits"], 
                                    res["hits"]["total"]["value"], 
                                    ["_source","system","cpu","idle","pct"]) / core_count
            if avg_cpu_idle < 0.20:
                if srv not in server_high_cpu:
                    server_high_cpu.append(srv)
                    res = es.search(index="metricbeat*", body=elastic_helper.get_general(srv[0], "process"))
                    services = group_services(res["hits"]["hits"], "cpu")
                    for service in services:
                        service["cpu"] = service["cpu"] / core_count
                    channel.basic_publish(exchange='alarms', 
                                            routing_key='', 
                                            body=msg_make_cpu(services, srv[0], 1 - avg_cpu_idle, "windowsServers", "warning"))
            else:
                if srv in server_high_cpu:
                    server_high_cpu.remove(srv)
                    channel.basic_publish(exchange='alarms', 
                                        routing_key='', 
                                        body=json.dumps({'name': srv[0], 'message': "Server is no longer under load: " + srv[0], 'origin': "windowsServers", 'severity': 'Error', 'format': 'HTML'}))

if __name__ == '__main__':
    ssl_context = create_ssl_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
   
    with open('config.json', 'r') as f:
        config = json.load(f)

    while True:
        servers = db_helper.conf_loader_sql(config["database"]["server"], config["database"]["user"], config["database"]["password"], config["database"]["db_name"])
        logic(servers, config)
        time.sleep(60*5)