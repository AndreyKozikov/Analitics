import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm  # Импортируем библиотеку tqdm


# Функция для обработки листа Google Sheets
def process_google_sheet(sheet_name):
    df = pd.read_excel(xls, sheet_name=sheet_name)
    df.ffill(inplace=True)  # Заполнение пустых значений
    return sheet_name, df


# Функция для парсинга курсов валют с сайта ЦБ РФ
def fetch_currency_rates():
    url = 'https://cbr.ru/currency_base/daily/'
    response = requests.get(url)
    response.encoding = 'utf-8'

    # Парсинг HTML страницы
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'data'})

    if table is None:
        raise ValueError("Не удалось найти таблицу с курсами валют на странице.")

    # Извлечение заголовков таблицы
    headers = [header.text for header in table.find_all('th')]

    # Извлечение строк данных
    rows = []
    for row in table.find_all('tr')[1:]:  # Пропускаем заголовок
        columns = row.find_all('td')
        if len(columns) == 5:  # Убедимся, что мы получили все необходимые столбцы
            rows.append([column.text.strip().replace(',', '.') for column in columns])  # Заменяем запятую на точку

    # Преобразуем данные в DataFrame
    df = pd.DataFrame(rows, columns=headers)
    return 'Курсы валют', df


# Шаг 1: URL для экспорта Google Sheets в формате Excel
google_sheets_url = 'https://docs.google.com/spreadsheets/d/152JyksagijqyscnrFDc6Ez2VjT5MKNXpDOyc4PRlauw/export?format=xlsx'

# Скачиваем файл с Google Sheets
response = requests.get(google_sheets_url)
with open('data.xlsx', 'wb') as file:
    file.write(response.content)

# Шаг 2: Чтение Excel-файла с использованием pandas
xls = pd.ExcelFile('data.xlsx')

# Шаг 3: Создание нового Excel-файла для записи
file_path = 'processed_data.xlsx'
with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
    # Используем ThreadPoolExecutor для параллельной обработки
    with ThreadPoolExecutor() as executor:
        # Создаём прогресс-бар
        total_sheets = len(xls.sheet_names) + 1  # +1 для курсов валют
        google_sheet_futures = {executor.submit(process_google_sheet, sheet_name): sheet_name for sheet_name in
                                xls.sheet_names}

        # Запрос курсов валют
        currency_future = executor.submit(fetch_currency_rates)

        # Сохранение результатов с прогресс-баром
        for future in tqdm(google_sheet_futures.keys(), total=total_sheets, desc="Сохранение листов"):
            sheet_name, df = future.result()
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f'Лист "{sheet_name}" сохранён в {file_path}')

        # Сохранение курсов валют
        currency_sheet_name, currency_df = currency_future.result()
        currency_df.to_excel(writer, sheet_name=currency_sheet_name, index=False)
        print(f'Лист "{currency_sheet_name}" сохранён в {file_path}')
