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
            # Формируем запрос в Elasticsearch
            query = {
                "query": {
                    "term": {"user.keyword": user}  # Поиск по имени пользователя
                },
                "_source": ["user", "host_name", "message"]  # Нам нужны только эти поля
            }
            
            # Получаем результаты запроса
            response = es.search(index=index, body=query, size=count)
            
            # Обрабатываем каждый результат
            for hit in response['hits']['hits']:
                # Извлекаем данные
                username = hit['_source'].get('user')
                host_name = hit['_source'].get('host_name', 'Unknown')  # Если нет host_name, ставим "Unknown"
                message = hit['_source'].get('message', 'No message')
                
                # Добавляем результат в итоговый список
                output_data.append([username, host_name, message, index])

# Записываем итоговые данные в Excel
df_output = pd.DataFrame(output_data, columns=["username", "host_name", "message", "index"])
df_output.to_excel(OUTPUT_FILE, index=False)

print(f"Данные успешно сохранены в {OUTPUT_FILE}")
