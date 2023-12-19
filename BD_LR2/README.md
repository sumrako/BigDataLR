## Описание работы кода
### Начало работы
Код начинается с настройки среды Spark:

```python
from pyspark.sql import SparkSession
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.13.0 pyspark-shell'
sc = SparkSession.builder.appName("L2").master("local[*]").getOrCreate()
```
Этот участок кода создает SparkSession с именем приложения "L2" и использует локальный режим для обработки данных.

### Загрузка данных
```python
!hadoop fs -put datasets/programming-languages.csv data/programming-languages.csv
pro_lang = sc.read.csv("data/programming-languages.csv")
```
```python
!hadoop fs -put datasets/posts_sample.xml data/posts_sample.xml
posts_sample = sc.read.format("xml").options(rowTag="row").load('data/posts_sample.xml')
```
Данный код загружает данные из CSV-файла и XML-файла для последующей обработки и анализа.

### Обработка данных
```python
def language_detection(x):
    tag = None
    for language in pro_lang_list:
        if "<" + language.lower() + ">" in x._Tags.lower():
            tag = language
            break
    if tag is None:
        return None
    return (x._Id, tag)


def check_date(x, year): 
    start = datetime(year=year, month=1, day=1)
    end = datetime(year=year, month=12, day=31)
    CreationDate = x._CreationDate
    return CreationDate >= start and CreationDate <= end
```
Функция language_detection() приводит весь текст к нижнему регистру и исследует каждую строку на наличие названия языка программирования.
Если язык обнаружен, создается кортеж с информацией о нем, в противном случае возвращается значение None.
Функция check_date() выполняет проверку даты, поскольку требуется оценить промежуток времени от 2010 до 2020.

```python
final_result = {}
for year in range(2010, 2020):
    final_result[year] = posts_sample.rdd.filter(
        lambda x: x._Tags is not None and check_date(x, year)
    ).map(
        language_detection
    ).filter(
        lambda x: x is not None
    ).keyBy(
        lambda x: x[1]
    ).aggregateByKey(
        0,
        lambda x, y: x + 1,
        lambda x1, x2: x1 + x2
    ).sortBy(lambda x: x[1], ascending=False).toDF()
    final_result[year] = final_result[year].select(
        col("_1").alias("Programming_language"), 
        col("_2").alias(f"Number_of_mentions_in_{year}")
    ).limit(10)
    final_result[year].show()
```
Этот участок кода фильтрует строки, исключая пустые значения, и выбирает временной интервал с 2010 по 2019 год. 
Затем производится поиск языков программирования в каждой строке и удаляются пустые значения. 
В случае, если язык не обнаружен, он исключается из анализа. Далее подсчитывается количество упоминаний каждого языка программирования в каждом году и происходит сортировка по частоте упоминаний. 
Результат сортируется от наиболее часто упоминаемого к наименее.

### Завершение работы
```python
sc.stop()
```
Этот участок кода завершает работу Spark и останавливает SparkSession.

## Как использовать
Установите Apache Spark и настройте среду выполнения PySpark.
Запустите код из Jupyter Notebook или интерпретатора Python, поддерживающего Spark.
Изучите результаты анализа данных о языках программирования в каждом году.
