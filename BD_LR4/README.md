# Лабораторная 4. Zookeeper

## Задание
- запустить ZooKeeper,
- изучить директорию с установкой ZooKeeper,
- запустить интерактивную сессию ZooKeeper CLI и освоить её команды,
- научиться проводить мониторинг ZooKeeper,
- разработать приложение с барьерной синхронизацией, основанной на ZooKeeper,
- запустить и проверить работу приложения.

### Установка и запуск
Zookeeper устанавливался на Windows. С помощью zkServer.cmd был запущен сервер

Для запуска интерактивной сессии ZooKeeper CLI используем скрипт zkCli. С помощью команды help изучаем возможности CLi интерфейса.

![image](https://github.com/sumrako/BigDataLR/assets/67976572/f56c37b9-8e82-44ca-957c-130d049d53ec)

Научимся добавлять и удалять разные типы узлов znode, считывать и записывать данные в znode из CLI, разбираться в управлении конфигурациями на базовых примерах. Находясь в консоли CLI введем команду ls /. В результе получим список узлов в корне иерархической структуры данных ZooKeeper. В данном случае выводится один узел. Аналогично можно изучать некорневые узлы. Выведем список дочерних узлов /zookeeper:

![image](https://github.com/sumrako/BigDataLR/assets/67976572/314895f9-1e9b-4f3c-a45b-535d1649d673)

Теперь в корне создадим свой узел /mynode с данными "first_version" и проверим, что в корне появился новый узел.

![image](https://github.com/sumrako/BigDataLR/assets/67976572/0abbdc7b-77d5-48b6-b806-abde927b2185)

Изменим данные узла на "second_version":

![image](https://github.com/sumrako/BigDataLR/assets/67976572/d245a4ef-d326-4d41-8297-6a04afd0782f)

Создадим два нумерованных (sequential) узла в качестве дочерних mynode:

![image](https://github.com/sumrako/BigDataLR/assets/67976572/5ed2cf4e-214b-4362-85ef-429902f5abc3)

Установив флаг -s, мы определили, что узлы будут иметь уникальные номера. Этот метод создания узлов с уникальными именами помогает установить последовательность запросов к серверу.

Например, для отслеживания принадлежности клиентов к группе, можно воссоздать простой сценарий мониторинга в ZooKeeper через интерфейс командной строки.

В данном сценарии в корне файловой системы ZooKeeper будет создан узел с именем "mygroup". Несколько CLI-сессий будут эмулировать клиентов, добавляя себя в эту группу. Каждый клиент создает временный узел в иерархии "mygroup". Эти узлы автоматически удаляются при закрытии сессии.

Такой подход можно использовать для создания службы резолвера имен (DNS) для узлов в кластере. Каждый узел регистрирует свое имя и сохраняет свой URL или IP-адрес. В списке присутствуют только работающие узлы, а те, которые временно недоступны или аварийно завершили работу, отсутствуют. Таким образом, директория поддерживает актуальный список активных узлов с их адресами.

В рамках CLI-сессии мы создадим узел "mygroup" и откроем две новые CLI-сессии.

![image](https://github.com/sumrako/BigDataLR/assets/67976572/7d7d3e30-95fa-4ab2-8ceb-5cf284323c96)
![image](https://github.com/sumrako/BigDataLR/assets/67976572/00978fd4-4195-46b8-8b9d-e1bd030cc806)
![image](https://github.com/sumrako/BigDataLR/assets/67976572/7879124c-3377-48e7-aefa-279d62d27fd6)
![image](https://github.com/sumrako/BigDataLR/assets/67976572/a25a29c8-d270-4fb0-b4ce-d624f46c6e69)

Представим теперь, что одному из клиентов нужна информация о другом клиенте (к качестве клиентов могут выступать узлы кластера). Этот сценарий эмулируется получением информации командой get. Выберем консоль grue и обратимся к информации узла bleen.

![image](https://github.com/sumrako/BigDataLR/assets/67976572/dbadf0ed-555f-46ee-bb34-67484d4f5799)

Теперь эмулируем аварийное отключение любого клиента. Нажмем сочетание клавиш Ctrl + D в одной из консолей, создавшей эфимерный узел.
Проверим, что соответствующий узел пропал из mygroup. Изменение списка дочерних узлов может произойти не сразу — от 2 до 20 tickTime, значение которого можно посмотреть в zoo.cfg.

![image](https://github.com/sumrako/BigDataLR/assets/67976572/8d578d2c-0079-4929-b596-4b26eafc3216)
![image](https://github.com/sumrako/BigDataLR/assets/67976572/e377aec1-19d6-4491-bfde-284385ab31bd)

В заключении удалим узел /mygroup.

![image](https://github.com/sumrako/BigDataLR/assets/67976572/b52b2555-7071-430f-af85-5c60f91b96c5)

### Animal task
![image](https://github.com/sumrako/BigDataLR/assets/67976572/dfd6b7ef-ec41-4dd4-8780-675fe5758386)
![image](https://github.com/sumrako/BigDataLR/assets/67976572/4d47ffcd-9ab6-47c0-98fb-f350083a31fd)


### Philosophers

Результат:
```
Philosopher 0 is going to eat
Philosopher 4 is going to eat
Philosopher 1 is going to eat
Philosopher 2 is going to eat
Philosopher 3 is going to eat
Philosopher 2 picked up the left fork
Philosopher 2 picked up the right fork
Philosopher 5 picked up the left fork
Philosopher 5 picked up the right fork
Philosopher 4 picked up the left fork
Philosopher 5 put the right fork
Philosopher 5 put the loft fork and finished eating
Philosopher 4 picked up the right fork
Philosopher 5 is thinking
Philosopher 1 picked up the left fork
Philosopher 2 put the right fork
Philosopher 2 put the loft fork and finished eating
Philosopher 1 picked up the right fork
Philosopher 3 picked up the left fork
Philosopher 2 is thinking
Philosopher 4 put the right fork
Philosopher 1 put the right fork
Philosopher 1 put the loft fork and finished eating
Philosopher 4 put the loft fork and finished eating
Philosopher 3 picked up the right fork
Philosopher 1 is thinking
Philosopher 4 is thinking
Philosopher 4 is going to eat
Philosopher 5 picked up the left fork
Philosopher 5 picked up the right fork
Philosopher 1 is going to eat
Philosopher 2 picked up the left fork
Philosopher 5 put the right fork
Philosopher 5 put the loft fork and finished eating
Philosopher 1 picked up the left fork
Philosopher 5 is thinking
Philosopher 3 is going to eat
Philosopher 3 put the right fork
Philosopher 3 put the loft fork and finished eating
Philosopher 2 picked up the right fork
Philosopher 3 is thinking
Philosopher 4 picked up the left fork
Philosopher 4 picked up the right fork
Philosopher 2 put the right fork
Philosopher 2 put the loft fork and finished eating
Philosopher 1 picked up the right fork
Philosopher 2 is thinking
Philosopher 2 is going to eat
Philosopher 3 picked up the left fork
Philosopher 1 put the right fork
Philosopher 1 put the loft fork and finished eating
Philosopher 1 is thinking
Philosopher 4 put the right fork
Philosopher 4 put the loft fork and finished eating
Philosopher 3 picked up the right fork
Philosopher 4 is thinking
Philosopher 3 put the right fork
Philosopher 3 put the loft fork and finished eating
Philosopher 3 is thinking
```

### Двуфазный коммит протокол для high-available регистра
Результат:

```
Waiting others clients: []
Client 1 request commit
Client 0 request rollback
Client 2 request rollback
Client 4 request commit
Client 3 request commit
Check clients
Client 0 do commit
Client 1 do commit
Client 2 do commit
Client 3 do commit
Client 4 do commit
Waiting others clients: []
