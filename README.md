1. Скачивание и первичная подготовка данных осуществляется скриптами python: data_loader загружает данные и формирует таблицу Excel, data_preparation готовит данные для PowerBI (вычисляет необходимые данные, делает замену некорректных значений, добавляет необходимые столбцы и заполняет их)
2. Итогом формируется файл processed_data.xlsx, который уже подгружается в дашборд. 