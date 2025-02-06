from elasticsearch import Elasticsearch

ELASTIC_HOST = "http://localhost:9200"  # Адрес БД
ELASTIC_USER = "elastic"  # Логин
ELASTIC_PASSWORD = "your_password"  # Пароль

es = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD))

# Проверяем доступные индексы
indices = es.cat.indices(format="json")
print("Доступные индексы:", indices)

index_name = "your_index_name"

# Проверяем, существует ли индекс
if not any(idx["index"] == index_name for idx in indices):
    print(f"Ошибка: Индекс '{index_name}' не найден!")
    exit(1)

output_file_1 = "file1.txt"
output_file_2 = "file2.txt"

seen_hosts_1 = set()
seen_hosts_2 = set()

scroll_size = 1000  

query = {
    "query": {
        "bool": {
            "should": [
                {"term": {"TargetUserName": "sa"}},
                {"term": {"SubjectUserName": "sa"}}
            ]
        }
    }
}

# Тестовый запрос, чтобы проверить, есть ли данные
test_response = es.search(index=index_name, body={"query": {"match_all": {}}}, size=5)
print("Пример данных из индекса:", test_response)

response = es.search(
    index=index_name,
    body=query,
    size=scroll_size,
    scroll="2m"
)

# Проверяем, есть ли совпадения
total_hits = response["hits"]["total"].get("value", 0) if isinstance(response["hits"]["total"], dict) else response["hits"]["total"]
print(f"Найдено {total_hits} совпадений.")

if total_hits == 0:
    print("Ошибка: Данные по запросу не найдены!")
    exit(1)

scroll_id = response["_scroll_id"]

with open(output_file_1, "a", encoding="utf-8") as f1, open(output_file_2, "a", encoding="utf-8") as f2:
    while True:
        hits = response["hits"]["hits"]
        
        if not hits:
            print("Данные закончились, прерываем цикл.")
            break

        for hit in hits:
            print("Обрабатываем запись:", hit)  # Выводим запись для проверки структуры
            doc = hit.get("_source", {})  # Меняем event_data на _source, если данные хранятся там

            target_user = doc.get("TargetUserName", "")
            subject_user = doc.get("SubjectUserName", "")
            host_name = doc.get("host", {}).get("name", "Неизвестно")
            computer_name = doc.get("computer_name", "Неизвестно")

            if target_user == "sa" and host_name not in seen_hosts_1:
                f1.write(f"{host_name}, {computer_name}\n")
                seen_hosts_1.add(host_name)

            if subject_user == "sa" and host_name not in seen_hosts_2:
                f2.write(f"{host_name}, {computer_name}\n")
                seen_hosts_2.add(host_name)

        response = es.scroll(scroll_id=scroll_id, scroll="2m")

print("Обработка завершена!")
