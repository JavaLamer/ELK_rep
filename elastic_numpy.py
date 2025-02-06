from elasticsearch import Elasticsearch
import re
import pandas as pd

# Подключение к Elasticsearch
ELASTIC_HOST = "http://localhost:9200"
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "your_password"
INDEX_NAME = "your_index_name"

es = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD))

# Файлы
output_file_1 = "file1.csv"
output_file_2 = "file2.csv"

# Регулярные выражения для поиска ключей и значений
user_pattern = re.compile(r"(TargetUserName|SubjectUserName)\s*[:=]\s*(\S+)")
host_pattern = re.compile(r"(host|computer_name)\s*[:=]\s*(\S+)")

# Списки для хранения данных
data_1 = []
data_2 = []

# Запрос к Elasticsearch для получения логов
query = {
    "size": 1000,
    "query": {"match_all": {}}
}
response = es.search(index=INDEX_NAME, body=query, scroll="2m")
scroll_id = response["_scroll_id"]

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
            
            if target_user == "sa":
                data_1.append([host_name, computer_name])
            
            if subject_user == "sa":
                data_2.append([host_name, computer_name])
    
    response = es.scroll(scroll_id=scroll_id, scroll="2m")

# Создаем DataFrame и сохраняем в CSV
pd.DataFrame(data_1, columns=["host", "computer_name"]).drop_duplicates().to_csv(output_file_1, index=False)
pd.DataFrame(data_2, columns=["host", "computer_name"]).drop_duplicates().to_csv(output_file_2, index=False)

print("Обработка завершена!")
