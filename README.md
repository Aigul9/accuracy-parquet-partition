# accuracy-parquet-partition

Магистерский проект по разработке модуля сверки данных между файлом формата parquet в HDFS кластере и партицированной таблицей в СУБД.

## Стек

- Python
- Apache Airflow
- PostgreSQL
- Docker
- Apache Hadoop
- Pylint

## Смысл проекта

Спустя retention period необходимо данные из бд перелить в более холодное хранилище. Из-за различных проблем (например, с сетью) залитые файлы могут быть повреждены. Сравнивать построчно на большом объеме данных неразумно. Поэтому для сверки использован вероятностный алгоритм сверки больших данных - фильтр Блума с подсчетом (Counting Bloom Filter). 

## Техническая логика

Транзакционные данные льются в таблицу в PostgreSQL. Таблица партицирована по месяцам. Retention period - 3 месяца.

Поток стоит на расписании в Airflow и работает по следующему алгоритму:
1. Находим список партиций на основе системных таблиц pg_class, pg_inherits.
2. Находим устаревшие партиции на основе таблицы метаданных, где заранее задаем retention period.
3. Если устаревшая партиция была найдена, достаем ее из бд.
4. Заполняем структуру фильтра Блума с подсчетом.
5. Достаем файл партиции из HDFS. Если фильтр Блума не выявил расхождений - удаляем партицию из бд, т.к. она была передана корректно. Если расхождения есть - нотифицируем в телеграм о причине: файл не найден/поврежден/не прошел проверку.

## Визуализация флоу

![image](https://github.com/user-attachments/assets/c4dbe1e7-93aa-4307-8cae-402ac6c3385b)
