def get_general(server_name, metric):
    query = {
        "size": 50,
        "query": {
        "bool": {
        "must": [
            {
            "match_phrase": {
                "metricset.name": {
                "query": metric
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
        ],
        "should": [],
        "must_not": []
        }
    }
    }
    return query

