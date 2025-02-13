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

    # Инициализируем scroll запрос
    query = {
        "size": 1000,  # Количество документов за один раз
        "query": {
            "match_all": {}  # Или твой запрос для выборки данных
        },
        "_source": ["user.keyword"]  # Поля, которые нас интересуют
    }

    # Начинаем scroll сессию
    response = es.search(index=index, body=query, scroll="5m")

    scroll_id = response["_scroll_id"]
    hits = response["hits"]["hits"]

    # Обрабатываем первую партию данных
    while hits:
        for hit in hits:
            user = hit["_source"].get("user", "Unknown")
            if user not in user_logs:
                user_logs[user] = {}

            if index not in user_logs[user]:
                user_logs[user][index] = 0

            user_logs[user][index] += 1  # Увеличиваем счетчик

        # Получаем следующую партию данных с помощью scroll_id
        response = es.scroll(scroll_id=scroll_id, scroll="5m")
        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]

    # Записываем прогресс
    with open(USED_INDEXES_FILE, "a") as f:
        f.write(index + "\n")

    # Записываем пользователей в файл
    with open(USER_LOGS_FILE, "w") as f:
        for user, data in user_logs.items():
            f.write(f"{user}:{json.dumps(data)}\n")

print("Обработка завершена!")









import pandas as pd
from elasticsearch import Elasticsearch
import json

# Подключение к Elasticsearch
es = Elasticsearch("http://localhost:9200")  # Укажи свой адрес

# Путь к файлам
HOST_USER_FILE = "host_user.txt"  # Файл с хостами и пользователями
USER_LOGS_FILE = "user_logs.txt"  # Файл с данными о пользователях и индексах
OUTPUT_FILE = "user_data.xlsx"  # Выходной Excel файл

# Читаем файл с пользователями и индексами
user_logs = {}
with open(USER_LOGS_FILE, "r") as f:
    for line in f:
        user, data = line.strip().split(":", 1)
        user_logs[user] = json.loads(data)

# Читаем файл с хостами и пользователями
host_user_map = {}
with open(HOST_USER_FILE, "r") as f:
    for line in f:
        parts = line.strip().split(" ")
        host = parts[1]  # Второй элемент — это хост (например, rrb.com)
        user = parts[3]  # Четвертый элемент — это user (например, rootsu)
        host_user_map[host] = user

# Список для итоговых данных
output_data = []

# Проходим по каждому хосту и ищем данные в user_logs.txt
for host, user in host_user_map.items():
    if user in user_logs:
        # Получаем индексы для этого пользователя
        indexes = user_logs[user]
        
        # Для каждого индекса выполняем запрос
        for index, count in indexes.items():
            print(f"Обрабатываем индекс {index} для пользователя {user} (хост: {host})")

            # Формируем запрос в Elasticsearch
            query = {
                "query": {
                    "term": {"user.keyword": user}  # Поиск по имени пользователя
                },
                "_source": ["user", "host_name", "message"]  # Нам нужны только эти поля
            }

            # Инициализируем scroll-сессию
            response = es.search(index=index, body=query, size=1000, scroll="5m")
            scroll_id = response["_scroll_id"]
            hits = response["hits"]["hits"]

            # Обрабатываем первую партию данных
            while hits:
                for hit in hits:
                    # Извлекаем данные
                    username = hit['_source'].get('user')
                    host_name = hit['_source'].get('host_name', 'Unknown')  # Если нет host_name, ставим "Unknown"
                    message = hit['_source'].get('message', 'No message')
                    
                    # Добавляем результат в итоговый список
                    output_data.append([username, host_name, message, index])

                # Получаем следующую партию данных с помощью scroll_id
                response = es.scroll(scroll_id=scroll_id, scroll="5m")
                scroll_id = response["_scroll_id"]
                hits = response["hits"]["hits"]

# Записываем итоговые данные в Excel
df_output = pd.DataFrame(output_data, columns=["username", "host_name", "message", "index"])
df_output.to_excel(OUTPUT_FILE, index=False)

print(f"Данные успешно сохранены в {OUTPUT_FILE}")

