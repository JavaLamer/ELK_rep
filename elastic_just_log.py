from elasticsearch import Elasticsearch

ELASTIC_HOST = "http://localhost:9200"  # Адрес БД
ELASTIC_USER = "elastic"  # Логин
ELASTIC_PASSWORD = "your_password"  # Пароль

es = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD))

output_file = "logs.txt"
scroll_size = 1000  # Лимит строк
index_name = "your_index_name"

query = {
    "query": {
        "match_all": {}
    }
}

response = es.search(index=index_name, body=query, size=scroll_size)

with open(output_file, "w", encoding="utf-8") as f:
    for hit in response["hits"]["hits"]:
        f.write(f"{hit}\n")

print("Логи записаны в файл logs.txt")
