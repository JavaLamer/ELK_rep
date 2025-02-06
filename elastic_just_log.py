import os
from elasticsearch import Elasticsearch

# Подключение к БД
ELASTIC_HOST = "http://localhost:9200"
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "your_password"

es = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD))

# Настройки
scroll_size = 1000
index_name = "your_index_name"

# Проверяем, существует ли индекс
if not es.indices.exists(index=index_name):
    print(f"⚠️ Индекс '{index_name}' не найден. Создаю...")
    es.indices.create(index=index_name)
    print(f"✅ Индекс '{index_name}' создан.")

# Поиск с использованием scroll и фильтрацией по 'TargetUserName' или 'SubjectUserName'
query = {
    "size": scroll_size,
    "query": {
        "should":[
            {"term":{"TargetUserName":"sa"}},
            {"term":{"SubjectUserName":"sa"}}
        ]
    }
}

response = es.search(index=index_name, body=query, scroll="2m")

# Записываем логи
try:
    subject_user_file_exists = os.path.exists("SubjectUserName.txt")
    target_user_file_exists = os.path.exists("TargetUserName.txt")

    # Открываем файлы для записи
    with open("SubjectUserName.txt", "a" if subject_user_file_exists else "w", encoding="utf-8") as subject_file, \
         open("TargetUserName.txt", "a" if target_user_file_exists else "w", encoding="utf-8") as target_file:

        if not subject_user_file_exists:
            subject_file.write("1\n")
        if not target_user_file_exists:
            target_file.write("1\n")

        scroll_id = response["_scroll_id"]
        total_hits = response["hits"]["total"]["value"]
        print(f"🔍 Найдено {total_hits} записей с TargetUserName='sa' или SubjectUserName='sa', начинаю экспорт...")

        while len(response["hits"]["hits"]) > 0:
            for hit in response["hits"]["hits"]:
                subject_username = hit["_source"].get("SubjectUserName", "")
                target_username = hit["_source"].get("TargetUserName", "")

                # Фильтруем по SubjectUserName и TargetUserName
                if subject_username == "sa":
                    subject_file.write(f"{hit}\n")
                if target_username == "sa":
                    target_file.write(f"{hit}\n")

            # Получаем следующий кусок данных
            response = es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response["_scroll_id"]

        print(f"✅ Логи записаны в файлы SubjectUserName.txt и TargetUserName.txt")

except IOError as e:
    print(f"❌ Ошибка при работе с файлами: {e}")
