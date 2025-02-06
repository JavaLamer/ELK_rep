import os
from elasticsearch import Elasticsearch

# Подключение к БД
ELASTIC_HOST = "http://localhost:9200"
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "your_password"

es = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD))

# Настройки
output_file = "logs.txt"
scroll_size = 1000
index_name = "your_index_name"

# Проверяем, существует ли индекс
if not es.indices.exists(index=index_name):
    print(f"⚠️ Индекс '{index_name}' не найден. Создаю...")
    es.indices.create(index=index_name)
    print(f"✅ Индекс '{index_name}' создан.")

# Поиск с использованием scroll
query = {
    "size": scroll_size,
    "query": {
        "match_all": {}
    }
}

response = es.search(index=index_name, body=query, scroll="2m")

# Записываем логи
try:
    file_exists = os.path.exists(output_file)
    with open(output_file, "a" if file_exists else "w", encoding="utf-8") as f:
        if not file_exists:
            f.write("1\n")

        scroll_id = response["_scroll_id"]
        total_hits = response["hits"]["total"]["value"]
        print(f"🔍 Найдено {total_hits} записей, начинаю экспорт...")

        while len(response["hits"]["hits"]) > 0:
            for hit in response["hits"]["hits"]:
                f.write(f"{hit}\n")

            response = es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response["_scroll_id"]

        print(f"✅ Логи записаны в файл {output_file}")

except IOError as e:
    print(f"❌ Ошибка при работе с файлом {output_file}: {e}")
