from elasticsearch import Elasticsearch
import json
import os

# Подключение к БД
ELASTIC_HOST = "http://localhost:9200"
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "your_password"

es = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD))
LOGS_DIR = "D:\\code\\dir\\list"

USED_INDEXES_FILE = os.path.join(LOGS_DIR, "used_indexes.txt")
USER_LOGS_FILE = os.path.join(LOGS_DIR, "user_logs.txt")

# Загружаем уже обработанные индексы
if os.path.exists(USED_INDEXES_FILE):
    with open(USED_INDEXES_FILE, "r") as f:
        used_indexes = set(f.read().splitlines())
else:
    used_indexes = set()

# Загружаем уже обработанные данные пользователей
user_logs = {}
if os.path.exists(USER_LOGS_FILE):
    with open(USER_LOGS_FILE, "r") as f:
        for line in f:
            user, data = line.strip().split(":", 1)
            user_logs[user] = json.loads(data)

# Получаем список всех индексов
all_indexes = es.cat.indices(format="json")
index_names = [index["index"] for index in all_indexes]

for index in index_names:
    if index in used_indexes:
        continue  # Пропускаем уже обработанные индексы

    print(f"Обрабатываем индекс: {index}")

    # Запрос логов
    query = {
        "size": 0,
        "aggs": {
            "users": {
                "terms": {"field": "user.keyword", "size": 10000}
            }
        }
    }

    response = es.search(index=index, body=query)

    # Обрабатываем результат
    buckets = response["aggregations"]["users"]["buckets"]
    for bucket in buckets:
        user = bucket["key"]
        count = bucket["doc_count"]

        if user not in user_logs:
            user_logs[user] = {}

        user_logs[user][index] = count

    # Записываем прогресс
    with open(USED_INDEXES_FILE, "a") as f:
        f.write(index + "\n")

    with open(USER_LOGS_FILE, "w") as f:
        for user, data in user_logs.items():
            f.write(f"{user}:{json.dumps(data)}\n")

print("Обработка завершена!")
