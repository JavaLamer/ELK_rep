curl -X GET "http://your_elasticsearch_host:9200/your_index/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "term": {
      "TargetUserName.keyword": "sa"
    }
  },
  "size": 1000
}
'
