from elasticsearch import Elasticsearch


ELASTIC_HOST = "http://localhost:9200"  # Адрес БД
ELASTIC_USER = "elastic"  # Логин
ELASTIC_PASSWORD = "your_password"  # Пароль

es = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD))

indices = es.cat.indices(format="json")

output_file_1 = "file1.txt"  
output_file_2 = "file2.txt"  


seen_hosts_1 = set()
seen_hosts_2 = set()

scroll_size = 1000  

index_name = "your_index_name"

    

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

    
response = es.search(
        index=index_name,
        body=query,
        size=scroll_size,
        scroll="2m" 
    )


scroll_id = response["_scroll_id"]

with open(output_file_1, "a", encoding="utf-8") as f1, open(output_file_2, "a", encoding="utf-8") as f2:
    while True:
        hits = response["hits"]["hits"]
            
            
        if not hits:
            break
            
            
        for hit in hits:
            doc = hit.get("event_data", {})
                
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
