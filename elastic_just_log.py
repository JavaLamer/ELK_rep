from elasticsearch import Elasticsearch

ELASTIC_HOST = "http://localhost:9200"  # Адрес БД
ELASTIC_USER = "elastic"  # Логин
ELASTIC_PASSWORD = "your_password"  # Пароль

es = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD))

output_file = "logs.txt"
scroll_size = 1000  # Лимит строк
index_name = "your_index_name"

query = {
    "size": scroll_size,  # Перенесли size внутрь body
    "query": {
        "match_all": {}
    }
}

response = es.search(index=index_name, body=query)  # Убрали size как отдельный параметр

try:
    with open(output_file, "w", encoding="utf-8") as f:
        if f.writable():  # Проверяем, можно ли записывать в файл
            for hit in response["hits"]["hits"]:
                f.write(f"{hit}\n")
            print(f"✅ Логи записаны в файл {output_file}")
        else:
            print(f"❌ Ошибка: Файл {output_file} открыт только для чтения.")
except IOError as e:
    print(f"❌ Ошибка при работе с файлом {output_file}: {e}")
