from elasticsearch import Elasticsearch
import re

# Подключение к Elasticsearch
ELASTIC_HOST = "http://localhost:9200"
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "your_password"
INDEX_NAME = "your_index_name"

es = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD))

# Файлы
output_file_1 = "file1.txt"
output_file_2 = "file2.txt"

# Регулярные выражения для поиска ключей и значений
user_pattern = re.compile(r"(TargetUserName|SubjectUserName)\s*[:=]\s*(\S+)")
host_pattern = re.compile(r"(host|computer_name)\s*[:=]\s*(\S+)")

# Множества для хранения уникальных хостов
seen_hosts_1 = set()
seen_hosts_2 = set()

# Запрос к Elasticsearch для получения логов
query = {
    "size": 1000,
    "query": {"match_all": {}}
}
response = es.search(index=INDEX_NAME, body=query, scroll="2m")
scroll_id = response["_scroll_id"]

with open(output_file_1, "w", encoding="utf-8") as f1, open(output_file_2, "w", encoding="utf-8") as f2:
    while True:
        hits = response["hits"]["hits"]
        if not hits:
            break
        
        for hit in hits:
            log = hit.get("_source", "")
            log_str = str(log)  # Преобразуем в строку, если нужно
            
            users = user_pattern.findall(log_str)  # Найти всех TargetUserName и SubjectUserName
            hosts = host_pattern.findall(log_str)  # Найти все host и computer_name
            
            if users and hosts:
                user_dict = {key: value for key, value in users}
                host_dict = {key: value for key, value in hosts}
                
                target_user = user_dict.get("TargetUserName", "")
                subject_user = user_dict.get("SubjectUserName", "")
                host_name = host_dict.get("host", "Неизвестно")
                computer_name = host_dict.get("computer_name", "Неизвестно")
                
                if target_user == "sa" and host_name not in seen_hosts_1:
                    f1.write(f"{host_name}, {computer_name}\n")
                    seen_hosts_1.add(host_name)
                
                if subject_user == "sa" and host_name not in seen_hosts_2:
                    f2.write(f"{host_name}, {computer_name}\n")
                    seen_hosts_2.add(host_name)
        
        response = es.scroll(scroll_id=scroll_id, scroll="2m")

print("Обработка завершена!")
