from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError, ConnectionError, TransportError
from datetime import datetime

# Настройка подключения
ELASTIC_HOST = "http://172.10.12.31:9200"
ELASTIC_USER = "root"
ELASTIC_PASSWORD = "uzZS6e2rhf56"


es = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD))
size=5000
unique_hosts = set()

# Формируем запрос с scroll
body = {
    "size":size,
    "_source": ["host.name", "winlog.event_data.SubjectUserName"],
    "query": {
        "term": {
            "winlog.event_data.SubjectUserName": "sa"
        }
    }
}
try:
    # Получаем список всех индексов
    indices_response = es.cat.indices(h='index', s='index')
    
    # Создаем файл для записи результатов
    with open('logs.txt', 'w', encoding='utf-8') as f:
        # Записываем заголовок
        f.write("Отчет по пользователям sa\n")
        f.write("=" * 50 + "\n\n")
        
        unique_hosts = set()
        
        # Обрабатываем каждый индекс
        for index_name in indices_response.split():
            print(f"\nОбработка индекса: {index_name}")
            
            try:
                # Выполняем поиск с scroll для текущего индекса
                response = es.search(
                    index=index_name,
                    body=body,
                    scroll='5m',
                )
                
                # Получаем все документы из текущего индекса
                while True:
                    # Обрабатываем текущую порцию документов
                    for hit in response['hits']['hits']:
                        host_name = hit['_source']['host']['name']
                        username = hit['_source']['winlog']['event_data']['SubjectUserName']
                        
                        # Проверяем, был ли уже такой хост
                        if host_name not in unique_hosts:
                            unique_hosts.add(host_name)
                            
                            # Записываем в файл
                            f.write(f"Хост: {host_name}, Пользователь: {username}\n")
                            print(f"Записано: Хост: {host_name}, Пользователь: {username}")
                    
                    # Если документов больше нет - выходим из цикла текущего индекса
                    if len(response['hits']['hits']) == 0:
                        break
                    
                    # Получаем следующую порцию документов
                    response = es.scroll(
                        scroll_id=response['_scroll_id'],
                        scroll='2m'
                    )
                
            except Exception as e:
                print(f"Ошибка при обработке индекса {index_name}: {str(e)}")
                continue
        
        # Записываем итоговую информацию
        f.write("\n" + "=" * 50 + "\n")
        f.write(f"Всего найдено уникальных хостов: {len(unique_hosts)}\n")
        print(f"\nРезультаты успешно записаны в файл logs.txt")

except RequestError as e:
    print(f"Ошибка запроса: {str(e)}")
except ConnectionError as e:
    print(f"Ошибка подключения: {str(e)}")
except TransportError as e:
    print(f"Ошибка транспорта: {str(e)}")
except Exception as e:
    print(f"Неожиданная ошибка: {str(e)}")
