from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError, ConnectionError, TransportError
from datetime import datetime
import os
import pandas as pd
from openpyxl import load_workbook, Workbook

# Константы конфигурации
ELASTIC_HOST = "http://172.10.12.31:9200"
ELASTIC_USER = "root"
ELASTIC_PASSWORD = "uzZS6e2rhf56"
BASE_DIR = "D:\\code\\dir"
INDEXES_FILE = "indexes.txt"
EXCEL_FILE = os.path.join(BASE_DIR, "Sa_host_message_file.xlsx")
CSV_FILE = os.path.join(BASE_DIR, "Sa_host_message_file.csv")
BATCH_SIZE = 50000  # Размер пакета для записи

# Инициализируем клиент Elasticsearch
es = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD))

# Создаем рабочую директорию, если она не существует
os.makedirs(BASE_DIR, exist_ok=True)

def load_processed_indices():
    """Загружает список обработанных индексов из файла."""
    if os.path.exists(INDEXES_FILE):
        with open(INDEXES_FILE, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_processed_index(index_name):
    """Сохраняет обработанный индекс в файл."""
    with open(INDEXES_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{index_name}\n")

def load_existing_messages():
    """Загружает существующие (host_name, short_message) в set() для ускорения проверки дубликатов."""
    existing_data = set()
    if os.path.exists(CSV_FILE):
        try:
            df = pd.read_csv(CSV_FILE, usecols=['Хост', 'Короткое сообщение'])
            existing_data = set(zip(df['Хост'], df['Короткое сообщение']))
        except Exception as e:
            print(f"Ошибка загрузки CSV: {str(e)}")
    return existing_data

def append_to_csv(data_list):
    """Записывает данные в CSV партиями."""
    df = pd.DataFrame(data_list, columns=['Хост', 'Пользователь', 'Индекс', 'Короткое сообщение', 'Сообщение'])
    df.to_csv(CSV_FILE, mode='a', header=not os.path.exists(CSV_FILE), index=False, encoding='utf-8')
    print(f"[{datetime.now()}] Данные записаны в CSV")

def process_indices():
    """Обрабатывает новые индексы в Elasticsearch."""
    try:
        indices_response = es.cat.indices(h='index', s='index')
        all_indices = set(indices_response.split())
        processed_indices = load_processed_indices()
        new_indices = sorted(all_indices - processed_indices)
        
        if not new_indices:
            print(f"[{datetime.now()}] Нет новых индексов для обработки.")
            return
        
        existing_messages = load_existing_messages()
        
        for index_name in new_indices:
            try:
                query = {
                    "size": 10000,
                    "_source": ["message", "host.name", "winlog.event_data.SubjectUserName"],
                    "query": {
                        "bool": {
                            "must": [{"match": {"winlog.event_data.SubjectUserName": "sa"}}]
                        }
                    }
                }
                response = es.search(index=index_name, body=query, scroll='5m')
                scroll_id = response['_scroll_id']
                buffer_data = []
                print(f"[{datetime.now()}] Обрабатывается индекс: {index_name}")

                while response['hits']['hits']:
                    for hit in response['hits']['hits']:
                        source = hit.get('_source', {})
                        host_name = source.get('host', {}).get('name', 'Неизвестно')
                        message = source.get('message', '')
                        username = source.get('winlog', {}).get('event_data', {}).get('SubjectUserName', 'Неизвестно')
                        short_message = message.split('.')[0].strip() if '.' in message else message

                        if (host_name, short_message) not in existing_messages:
                            buffer_data.append([host_name, username, index_name, short_message, message])
                            existing_messages.add((host_name, short_message))  # Добавляем в кэш

                        if len(buffer_data) >= BATCH_SIZE:
                            append_to_csv(buffer_data)
                            buffer_data.clear()
                    
                    response = es.scroll(scroll_id=scroll_id, scroll='5m')
                
                if buffer_data:
                    append_to_csv(buffer_data)
                
                save_processed_index(index_name)
                print(f"[{datetime.now()}] Индекс {index_name} обработан.")
            
            except (TransportError, RequestError, ConnectionError) as e:
                print(f"[{datetime.now()}] Ошибка при обработке {index_name}: {str(e)}")
            except Exception as e:
                print(f"[{datetime.now()}] Непредвиденная ошибка в индексе {index_name}: {str(e)}")
    
    except Exception as e:
        print(f"[{datetime.now()}] Критическая ошибка при получении индексов: {str(e)}")

if __name__ == "__main__":
    process_indices()
