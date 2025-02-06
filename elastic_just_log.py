import os
from elasticsearch import Elasticsearch

# Подключение к БД
ELASTIC_HOST = "http://localhost:9200"
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "your_password"

es = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD))

# Настройки
output_file = "logs.txt"
index_name = "your_index_name"

# Проверяем, существует ли индекс
if not es.indices.exists(index=index_name):
    print(f"⚠️ Индекс '{index_name}' не найден. Создаю...")
    es.indices.create(index=index_name)
    print(f"✅ Индекс '{index_name}' создан.")

# Получаем данные из Elasticsearch
response = es.search(index=index_name, body={
    "_source": ["host.name", "winlogon.event_data.SubjectUserName"],
    "query": {
        "term": {"winlogon.event_data.SubjectUserName": "sa"}
    },
    "size": 50
})

# Загружаем уже записанные хосты
if os.path.exists(output_file):
    with open(output_file, "r") as file:
        recorded_hosts = set(line.strip() for line in file)
else:
    recorded_hosts = set()

# Проверяем и записываем новые хосты
new_hosts = set()
for hit in response['hits']['hits']:
    host_name = hit.get("_source", {}).get("host", {}).get("name")
    if host_name and host_name not in recorded_hosts:
        new_hosts.add(host_name)

# Записываем новые хосты в файл
if new_hosts:
    with open(output_file, "a") as file:
        for host in new_hosts:
            file.write(host + "\n")
    print(f"✅ Добавлены новые хосты: {', '.join(new_hosts)}")
else:
    print("ℹ️ Новых хостов для записи нет.")
