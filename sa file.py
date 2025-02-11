def process_files(file1, file2, file3):
    seen_lines = set()
    
    # Читаем строки из второго файла в множество для быстрого поиска
    with open(file2, 'r', encoding='utf-8') as f2:
        lines_file2 = set(f2.read().splitlines())
    
    # Обрабатываем строки из первого файла
    with open(file1, 'r', encoding='utf-8') as f1, open(file3, 'w', encoding='utf-8') as f3:
        for line in f1:
            line = line.strip()
            if line not in seen_lines:  # Проверяем, не записывали ли уже эту строку
                seen_lines.add(line)  # Добавляем строку в множество записанных
                f3.write(line + '\n')

# Пример использования
process_files('file1.txt', 'file2.txt', 'file3.txt')
