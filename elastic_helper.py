def get_cpu_query(server_name):
    query = {
        "size": 50,
        "query": {
        "bool": {
        "must": [
            {
            "match_phrase": {
                "metricset.name": {
                "query": "cpu"
                }
            }
            },
            {
            "match_phrase": {
                "beat.name": {
                "query": server_name
                }
            }
            },
            {
            "range": {
                "@timestamp": {
                "gte": "now-1m"
                }
            }
            }
        ],
        "filter": [
            {
            "bool": {
                "should": [
                {
                    "match": {
                    "host.name": server_name
                    }
                }
                ],
                "minimum_should_match": 1
            }
            }
        ],
        "should": [],
        "must_not": []
        }
    }
    }
    return query

def get_memory_query(server_name):
    query = {
        "size": 50,
        "query": {
            "bool": {
            "must": [
                {
                "match_phrase": {
                    "metricset.name": {
                    "query": "memory"
                    }
                }
                },
                {
                "match_phrase": {
                    "beat.name": {
                    "query": server_name
                    }
                }
                },
                {
                "range": {
                    "@timestamp": {
                    "gte": "now-1m",
                    }
                }
                }
            ],
            "filter": [
            ],
            "should": [],
            "must_not": []
            }
        }
    }
    return query

def get_processes_query(server_name):
    query = {
        "size": 50,
        "query": {
            "bool": {
            "must": [
                {
                "match_phrase": {
                    "metricset.name": {
                    "query": "process"
                    }
                }
                },
                {
                "match_phrase": {
                    "beat.name": {
                    "query": server_name
                    }
                }
                },
                {
                "range": {
                    "@timestamp": {
                    "gte": "now-1m",
                    }
                }
                }
            ],
            "filter": [
            ],
            "should": [],
            "must_not": []
            }
        }
    }
    return query