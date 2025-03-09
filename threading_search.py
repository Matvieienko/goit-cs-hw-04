import os
import threading
import time
from queue import Queue
from collections import defaultdict

def search_keywords_in_files(files, keywords, result_queue):
    """
    Пошук ключових слів у списку файлів. Кожен файл обробляється окремо,
    результати зберігаються у локальному словнику та передаються через чергу.
    """
    local_results = defaultdict(list)  # Локальний словник для результатів пошуку
    for file_path in files:
        if not os.path.exists(file_path):
            print(f"Файл {file_path} не знайдено!")
            continue

        print(f"Пошук у {file_path}...")  # Логування прогресу

        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
                for keyword in keywords:
                    if keyword.lower() in content.lower():  # Приводимо до нижнього регістру
                        local_results[keyword].append(file_path)  # Додаємо знайдене ключове слово
        except Exception as e:
            print(f"Помилка обробки файлу {file_path}: {repr(e)}")  # Обробка помилок

    result_queue.put(local_results)  # Додаємо результати в чергу

def threaded_search(file_list, keywords, num_threads=4):
    """
    Основна функція для багатопотокового пошуку ключових слів у файлах.
    Розподіляє файли між потоками, збирає результати та повертає фінальний словник.
    """
    result_queue = Queue()  # Черга для збору результатів
    threads = []
    num_threads = min(num_threads, len(file_list))  # Обмеження кількості потоків
    chunk_size = len(file_list) // num_threads  # Розмір частини файлів для кожного потоку

    # Розподіл файлів між потоками
    for i in range(num_threads):
        start_idx = i * chunk_size
        end_idx = (i + 1) * chunk_size if i != num_threads - 1 else len(file_list)
        thread = threading.Thread(
            target=search_keywords_in_files,
            args=(file_list[start_idx:end_idx], keywords, result_queue)
        )
        threads.append(thread)
        thread.start()

    # Очікуємо завершення всіх потоків
    for thread in threads:
        thread.join()

    # Збираємо результати з черги
    final_results = defaultdict(list)
    while not result_queue.empty():  # Переконуємось, що в черзі є дані перед get()
        local_results = result_queue.get()
        for keyword, paths in local_results.items():
            final_results[keyword].extend(paths)

    return final_results 

if __name__ == "__main__":
    # Список файлів та ключових слів для пошуку
    file_paths = ["text_1.txt", "text_2.txt", "text_3.txt", "text_4.txt"]
    keywords = ["python", "logging", "indexing", "nonexistent"]

    # Вимірюємо час виконання
    start_time = time.perf_counter()
    results = threaded_search(file_paths, keywords, num_threads=4)
    end_time = time.perf_counter()

    # Виведення результатів
    for keyword in keywords:
        if keyword in results and results[keyword]:
            print(f"Ключове слово '{keyword}' знайдено у файлах: {', '.join(results[keyword])}")
        else:
            print(f"Ключове слово '{keyword}' не знайдено у файлах.")

    print(f"Час виконання: {end_time - start_time:.4f} секунд")