## Содержание

<details>
<summary>

### 1. Фундамент и архитектура Airflow
</summary>

- **Введение в оркестрацию данных**
  - Что такое оркестрация и зачем она нужна
  - Проблемы cron-скриптов и shell-скриптов
  - Airflow в экосистеме Data Engineering (альтернативы: Luigi, Prefect, Dagster, Oozie)
- **История и философия Airflow**
  - Создание в Airbnb (2014), инкубация в Apache
  - Основные принципы: динамическое создание DAG'ов, декларативность, расширяемость
  - Сквозное понимание: "DAG as code"
- **Архитектура Airflow**
  - **Компоненты:**
    - **Web Server:** UI, визуализация DAG'ов, управление
    - **Scheduler:** планирование и запуск задач
    - **Worker:** выполнение задач (в Celery/Kubernetes режимах)
    - **Executor:** что выполняет задачи (Sequential, Local, Celery, Kubernetes)
    - **Metadata Database:** хранение состояния (PostgreSQL, MySQL, SQLite)
    - **DAG Directory:** хранилище файлов DAG'ов
  - **Работа компонентов:** как Scheduler находит и планирует задачи
  - **Архитектурные паттерны:**
    - Single-node (LocalExecutor)
    - Distributed (CeleryExecutor, KubernetesExecutor)
    - Celery vs Kubernetes Executor — когда что выбирать
- **Установка и конфигурация**
  - Установка через pip (apache-airflow)
  - Настройка подключения к базе данных
  - Инициализация базы: `airflow db init`
  - Создание пользователя: `airflow users create`
  - Запуск компонентов: `airflow webserver`, `airflow scheduler`, `airflow worker`
  - Production-ready установка: systemd, Docker, официальный Helm-чарт
- **Конфигурационные файлы**
  - `airflow.cfg` — основные настройки
  - Переменные окружения для переопределения (`AIRFLOW__CORE__DAGS_FOLDER`)
  - Подключение к внешним системам через Connections
  - Политика параллелизма: `parallelism`, `dag_concurrency`, `max_active_runs_per_dag`
</details>

<details>
<summary>

### 2. Основы DAG'ов (Directed Acyclic Graphs)
</summary>

- **Структура DAG**
  - Импорты: `from airflow import DAG`
  - Параметры DAG: `dag_id`, `description`, `start_date`, `schedule_interval`, `catchup`, `tags`
  - Контекст DAG: `default_args` для всех задач
  - Документирование DAG: `doc_md`
- **Операторы (Operators)**
  - **Базовые:**
    - `PythonOperator` — выполнение Python-функций
    - `BashOperator` — выполнение bash-команд
    - `DummyOperator` — заглушка
  - **Системные:**
    - `EmailOperator` — отправка email
    - `BranchPythonOperator` — ветвление
    - `ShortCircuitOperator` — остановка выполнения
    - `LatestOnlyOperator` — выполнение только в последнем запуске
  - **Сенсоры (Sensors):**
    - `FileSensor` — ожидание файла
    - `HttpSensor` — ожидание ответа HTTP
    - `SqlSensor` — ожидание данных в SQL
    - `TimeSensor` / `TimeDeltaSensor` — ожидание времени
    - `ExternalTaskSensor` — ожидание завершения задачи в другом DAG
- **Зависимости (Dependencies)**
  - `>>` и `<<` операторы: `task1 >> task2`
  - `set_upstream()` и `set_downstream()`
  - `cross_downstream()` — множественные зависимости
  - `chain()` — линейные цепочки
- **Контекст выполнения (Jinja Templating)**
  - Переменные шаблонизации: `{{ ds }}`, `{{ execution_date }}`, `{{ prev_ds }}`, `{{ next_ds }}`
  - Макросы: `macros.datetime`, `macros.timedelta`
  - Доступ к конфигурации: `{{ var.value.my_var }}`
  - Доступ к подключениям: `{{ conn.my_conn_id }}`
- **Параметры DAG Run**
  - `execution_date` (логическая дата) vs `start_date` (физическая дата запуска)
  - `data_interval_start` / `data_interval_end` (Airflow 2.2+)
  - `logical_date` (Airflow 2.4+)
- **Catchup и Backfill**
  - `catchup=False` — пропуск пропущенных интервалов
  - `catchup=True` — выполнение всех пропущенных
  - Команда `airflow dags backfill` — повторный запуск за период
</details>

<details>
<summary>

### 3. Продвинутые концепции DAG'ов
</summary>

- **Динамические DAG'и**
  - Генерация DAG'ов на основе конфигурации (YAML/JSON)
  - Генерация задач внутри одного DAG (циклы for)
  - Проблемы: слишком много задач, производительность парсинга
- **TaskFlow API (Airflow 2.0+)**
  - Декоратор `@task` для упрощенного создания Python-задач
  - Автоматическая передача данных между задачами (XCom)
  - Композиция задач через функции
  - Поддержка виртуальных окружений и зависимостей
- **XCom (Cross-Communication)**
  - Передача данных между задачами
  - Методы: `task_instance.xcom_push()`, `task_instance.xcom_pull()`
  - Ограничения: размер данных (не более 48KB в метабазе)
  - Стратегии: передача ссылок на внешние хранилища (S3, HDFS) вместо больших данных
- **Branching (Ветвление)**
  - `BranchPythonOperator` — выбор следующей задачи
  - `BranchSQLOperator` — ветвление на основе SQL-запроса
  - `BranchDayOfWeekOperator` — ветвление по дню недели
  - Обработка триггер-правил (trigger rules) при ветвлении
- **Trigger Rules (Правила запуска)**
  - `all_success` (по умолчанию)
  - `all_failed`, `one_success`, `one_failed`, `all_done`, `none_failed`, `none_failed_min_one_success`, `none_skipped`, `always`
  - Применение в сложных ветвлениях и обработке ошибок
- **Task Groups (Группы задач)**
  - Визуальная группировка задач (Airflow 2.0+)
  - Вложенные Task Groups
  - Повторное использование групп задач
  - Динамические Task Groups
- **SubDAGs (устаревший подход)**
  - Проблемы SubDAG: производительность, сложность, deadlocks
  - Миграция на Task Groups
- **Сложные зависимости**
  - Циклические зависимости — невозможно в DAG
  - Условные зависимости через `BranchPythonOperator` и `trigger_rule`
</details>

<details>
<summary>

### 4. Операторы для Data Engineering
</summary>

- **Работа с базами данных**
  - `SqlOperator` / `SQLExecuteQueryOperator`
  - `PostgresOperator`, `MySqlOperator`, `MsSqlOperator`
  - `SnowflakeOperator`, `BigQueryOperator`, `RedshiftOperator`
  - `SqliteOperator`
  - Специфические: `S3ToRedshiftOperator`, `BigQueryToGCSOperator`
- **Работа с Hadoop экосистемой**
  - `HiveOperator`, `PigOperator`
  - `SparkSubmitOperator` — запуск Spark-приложений
  - `SparkJDBCOperator` — чтение из JDBC в Spark
  - `HdfsSensor`, `HdfsHook`
- **Работа с облачными хранилищами**
  - **AWS:** `S3Operator`, `S3FileTransformOperator`, `S3ToRedshiftOperator`, `EMR operators`
  - **GCP:** `GCSOperator`, `BigQueryOperator`, `DataflowOperator`
  - **Azure:** `AzureBlobStorageOperator`, `AzureDataFactoryOperator`
- **Работа с Kafka и стримингом**
  - `KafkaConsumerSensor`
  - `KafkaProducerOperator` (через пользовательские операторы)
- **Работа с Docker и Kubernetes**
  - `DockerOperator` — запуск задач в Docker-контейнерах
  - `KubernetesPodOperator` — запуск задач в Kubernetes-подах (для изоляции)
- **Работа с API и HTTP**
  - `SimpleHttpOperator` — выполнение HTTP-запросов
  - `HttpSensor` — ожидание ответа от API
- **Работа с файлами**
  - `FileSensor` — ожидание появления файла
  - `FTPOperator`, `SFTPOperator`
  - `LocalFilesystemToGCSOperator` и аналоги
- **Работа с уведомлениями**
  - `EmailOperator`
  - `SlackOperator` / `SlackAPIOperator`
  - `TelegramOperator` (пользовательские)
  - `PagerDutyOperator` — инциденты
</details>

<details>
<summary>

### 5. Планирование (Scheduling) и Календари
</summary>

- **Параметры расписания**
  - `schedule_interval`: `@daily`, `@hourly`, `@weekly`, `@monthly`, `@once`, `None`
  - CRON-выражения: `0 0 * * *`
  - Timedelta: `timedelta(days=1)`
  - Сложные расписания через `cron_presets`
- **Timetables (Airflow 2.2+)**
  - Кастомные расписания (например, каждый вторник и четверг)
  - Создание собственного Timetable класса
  - `CronTriggerTimetable`, `DeltaTriggerTimetable`
- **Data-aware scheduling (Airflow 2.4+)**
  - Dataset как источник триггера
  - `Dataset` и `outlets` / `inlets`
  - Продвинутая оркестрация на основе обновления данных
- **Backfill и Catchup (углубленно)**
  - Как работает backfill под капотом
  - Ограничение параллельных backfill'ов
  - Очистка старых DAG Run'ов
- **Пропуски и остановки**
  - `catchup=False` и пропуск интервалов
  - Остановка DAG: `is_paused_upon_creation`
  - Ручной триггер DAG (UI или CLI)
</details>

<details>
<summary>

### 6. Пул соединений и переменные (Connections & Variables)
</summary>

- **Connections (Подключения)**
  - Хранение credentials в зашифрованном виде
  - Типы: `aws`, `gcp`, `azure`, `postgres`, `http`, `ssh`, `docker`, `spark`
  - Создание через UI, CLI или переменные окружения
  - Доступ в коде: `connection = BaseHook.get_connection('conn_id')`
  - Использование в операторах: параметр `postgres_conn_id`
- **Variables (Переменные)**
  - Хранение конфигурационных параметров
  - Создание через UI, CLI или JSON-файл
  - Доступ: `Variable.get('var_name')`
  - Переменные с дефолтным значением
  - Сериализация/десериализация JSON
- **Пуллы ресурсов (Pools)**
  - Ограничение параллельного выполнения задач
  - Приоритеты задач (priority_weight)
  - Слоты и ожидание
  - Мониторинг загрузки пулов
- **Хуки (Hooks)**
  - Абстракция для подключения к внешним системам
  - Пример: `S3Hook`, `PostgresHook`, `HttpHook`
  - Создание собственных хуков
</details>

<details>
<summary>

### 7. Тестирование и качество DAG'ов
</summary>

- **Тестирование DAG'ов**
  - Проверка синтаксиса: `python dags/my_dag.py`
  - Проверка импортов: `airflow dags list`
  - Юнит-тесты для DAG'ов (pytest)
    - Проверка структуры: количество задач, зависимости
    - Проверка отсутствия циклов
    - Проверка конфигурации задач
- **Тестирование задач**
  - Выполнение задач изолированно
  - Мокирование внешних подключений (pytest-mock)
  - Тестирование с реальными подключениями (Testcontainers)
- **Проверка качества**
  - Линтеры: `pylint`, `flake8` для DAG-файлов
  - Статический анализ: `airflow dags test`
  - Проверка best practices (например, `dag.test()` в Airflow 2.5+)
- **CI/CD для DAG'ов**
  - Тестирование в CI-пайплайне
  - Синхронизация DAG-файлов с продакшеном (git-sync, rsync)
  - Версионирование DAG'ов и обратная совместимость
</details>

<details>
<summary>

### 8. Мониторинг, логирование и оповещения
</summary>

- **Мониторинг DAG'ов и задач**
  - Статусы: success, failed, running, queued, skipped, upstream_failed
  - Метрики в UI: длительность, успешность, задержки
  - Интеграция с Prometheus: `airflow metrics`
  - Инструменты: Flower (для Celery), Grafana дашборды
- **Логирование**
  - Где хранятся логи: локально, в S3/GCS, в Elasticsearch
  - Настройка удаленного хранения логов (`remote_logging`)
  - Просмотр логов в UI
  - Структура логов: task logs, scheduler logs, webserver logs
- **Оповещения (Alerts)**
  - Email alerts при падении задач
  - Slack/Telegram/PagerDuty интеграции
  - Callbacks: `on_success_callback`, `on_failure_callback`, `on_retry_callback`
  - SLA Misses: `sla_miss_callback`
  - Оповещения о пропущенных SLAs
- **Метрики производительности**
  - Время парсинга DAG'ов
  - Загрузка scheduler'а
  - Очереди задач
  - Использование пулов
</details>

<details>
<summary>

### 9. Масштабирование и производительность
</summary>

- **Оптимизация парсинга DAG'ов**
  - Проблема: слишком много DAG'ов или задач
  - Кэширование DAG'ов: `dagbag` и `dag_processor_manager`
  - Настройка `min_file_process_interval`, `dag_dir_list_interval`
  - Использование `DagFileProcessorManager`
- **Выбор Executor'а**
  - **LocalExecutor:** для небольших инсталляций
  - **CeleryExecutor:** классика для продакшена
    - Настройка брокера (Redis, RabbitMQ)
    - Настройка бекенда результатов (Redis, DB)
    - Масштабирование workers
  - **KubernetesExecutor:** динамическое создание подов под каждую задачу
    - Изоляция, масштабируемость, эффективность ресурсов
    - Настройка pod_template
  - **CeleryKubernetesExecutor:** гибрид (Airflow 2.0+)
- **Оптимизация базы данных**
  - Индексы в метабазе
  - Очистка старых записей: `airflow db clean`
  - Партиционирование таблиц (для больших инсталляций)
- **Настройка параллелизма**
  - `parallelism` (глобальный)
  - `dag_concurrency` (на DAG)
  - `max_active_tasks_per_dag`
  - `worker_concurrency` (для Celery)
- **High Availability (Отказоустойчивость)**
  - Несколько scheduler'ов (Airflow 2.0+) — HA режим
  - Репликация базы данных
  - Несколько веб-серверов за балансировщиком
  - Обработка сбоев workers
</details>

<details>
<summary>

### 10. Безопасность и управление доступом
</summary>

- **Аутентификация**
  - Встроенная: пароли (по умолчанию)
  - LDAP / Active Directory
  - OAuth (Google, GitHub)
  - OpenID Connect
  - SAML
- **Авторизация (RBAC)**
  - Роли: Admin, User, Op, Viewer
  - Создание кастомных ролей
  - Ограничение доступа к DAG'ам (dag-level permissions)
  - Ограничение доступа к Connections и Variables
- **Шифрование**
  - Шифрование подключений и переменных в метабазе (Fernet key)
  - Настройка `fernet_key`
  - TLS для веб-сервера и API
  - Шифрование логов
- **Аудит**
  - Логирование действий пользователей
  - Audit logs в базе данных
  - Интеграция с внешними SIEM системами
- **Сетевая безопасность**
  - Изоляция workers
  - Ограничение доступа к API
  - CORS настройки
</details>

<details>
<summary>

### 11. API и интерфейсы
</summary>

- **REST API (Airflow 2.0+)**
  - Автоматическая генерация Swagger (OpenAPI)
  - Эндпоинты: DAGs, DAG Runs, Tasks, Connections, Variables, Pools
  - Аутентификация через API ключи или JWT
  - Управление DAG'ами через API (запуск, остановка)
- **CLI (Command Line Interface)**
  - Основные команды: `dags`, `tasks`, `db`, `users`, `connections`, `variables`, `pools`
  - Примеры: `airflow dags trigger`, `airflow tasks test`, `airflow db upgrade`
- **GraphQL (через сторонние пакеты)**
- **Python API**
  - Программное создание DAG'ов
  - Взаимодействие через `Airflow` класс
</details>

<details>
<summary>

### 12. Плагины и кастомизация
</summary>

- **Архитектура плагинов**
  - Структура папки `plugins`
  - Точки расширения: Operators, Hooks, Sensors, Views, Macros, Menu links
- **Создание собственных операторов**
  - Наследование от `BaseOperator`
  - Переопределение `execute` метода
  - Параметры, шаблонизация, XCom
- **Создание собственных хуков**
  - Наследование от `BaseHook`
  - Работа с подключениями
- **Создание собственных сенсоров**
  - Наследование от `BaseSensorOperator`
  - Параметры `poke_interval`, `timeout`, `mode` (poke vs reschedule)
- **Создание собственных макросов**
  - Добавление функций в Jinja-контекст
- **Создание кастомных View (Flask AppBuilder)**
  - Расширение UI
- **Установка и управление плагинами**
</details>

<details>
<summary>

### 13. Интеграция с современным Data Stack
</summary>

- **Airflow + dbt (Data Build Tool)**
  - `DbtRunOperator`, `DbtTestOperator`
  - Управление трансформациями данных
- **Airflow + Great Expectations**
  - Проверка качества данных в пайплайнах
- **Airflow + Data Lake (Iceberg, Hudi, Delta)**
  - Запуск Spark-задач для компактизации
  - Обновление метаданных
- **Airflow + Kafka / Streaming**
  - Запуск микробатчей
  - Обработка потоков через Spark Structured Streaming
- **Airflow + ML (Machine Learning)**
  - Пайплайны обучения моделей
  - `PythonOperator` с ML-библиотеками
  - Интеграция с MLflow, Kubeflow
- **Airflow + Data Observability**
  - Отправка метаданных в DataHub, Amundsen
  - Интеграция с OpenLineage ( lineage tracking)
</details>

<details>
<summary>

### 14. Лучшие практики и антипаттерны
</summary>

- **Best Practices**
  - Используйте TaskFlow API для Python-задач
  - Избегайте слишком большого количества задач в одном DAG
  - Используйте сенсоры в режиме `reschedule` вместо `poke` для долгих ожиданий
  - Храните конфигурацию в Variables, а не в коде DAG
  - Используйте Connection для credentials
  - Пишите тесты для DAG'ов
  - Документируйте DAG'и (doc_md)
  - Используйте четкие имена для DAG'ов и задач
  - Настраивайте retries для задач (retries, retry_delay)
  - Используйте pools для ограничения ресурсоемких задач
  - Мониторьте время выполнения и SLA
- **Антипаттерны (Чего НЕ делать)**
  - Тяжелые вычисления в операторах (используйте SparkOperator)
  - Передача больших данных через XCom
  - Слишком частый парсинг DAG'ов (короткий `min_file_process_interval`)
  - Игнорирование версионирования DAG'ов
  - Хранение секретов в коде
  - Использование SubDAGs (устарело)
  - Слишком много DAG'ов (тысячи) — проблемы с парсингом
  - Задачи, которые могут выполняться бесконечно (нет таймаутов)
  - Игнорирование обработки ошибок в задачах
</details>

<details>
<summary>

### 15. Airflow для Senior: Собеседование и кругозор
</summary>

- **Архитектурные вопросы**
  - Как работает Scheduler под капотом?
  - В чем разница между CeleryExecutor и KubernetesExecutor?
  - Как обеспечить High Availability для Airflow?
  - Как масштабировать Airflow для тысяч DAG'ов?
  - Как обрабатывать пиковые нагрузки?
  - Как настроить мониторинг и оповещения?
- **Практические кейсы (Problem Solving)**
  - "У нас DAG'и не успевают выполняться за окно. Ваши действия?"
  - "Scheduler тормозит и не планирует задачи вовремя"
  - "Как организовать пайплайн с зависимостью от обновления данных в S3?"
  - "Нужно перезапустить упавшие задачи за последние 3 месяца"
  - "Как обеспечить идемпотентность DAG'ов?"
- **Миграция и обновление**
  - Миграция с Airflow 1.x на 2.x
  - Обновление версий (backward compatibility)
  - Миграция базы данных
- **Тренды и будущее**
  - Data-aware scheduling
  - Улучшение производительности (AIP-43, AIP-44)
  - Интеграция с OpenLineage
  - Airflow как часть Data Mesh
</details>
