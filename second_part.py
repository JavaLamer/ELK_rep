import pandas as pd
from elasticsearch import Elasticsearch
import json

# Подключение к Elasticsearch
es = Elasticsearch("http://localhost:9200")  # Укажи свой адрес

# Путь к файлу с пользователями и индексами
USER_LOGS_FILE = "user_logs.csv"  # В случае CSV
OUTPUT_FILE = "user_data.xlsx"  # Выходной Excel файл

# Читаем файл CSV
df = pd.read_csv(USER_LOGS_FILE)

# Список для итоговых данных
output_data = []

# Проходим по всем строкам в DataFrame
for _, row in df.iterrows():
    user = row['user']
    indexes = json.loads(row['index'])  # Переводим строку JSON в словарь
    for index, count in indexes.items():
        # Делаем запрос для каждого индекса
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
