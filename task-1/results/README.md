# Задание 1. Пакетная обработка данных

## Выбранное решение: Apache Airflow

Пайплайн объединяет CSV со статусами доставок, таблицы PostgreSQL, платежи и события из Kafka,
примерно 1 млн записей за запуск, и должен уметь ветвиться, ретраиться и уведомлять.
Airflow закрывает всё это штатными средствами, поэтому берём его.

Рассматривал и отложил:
- Spring Batch: хорош для чанковой обработки одной БД/файла, но ветвление между шагами, email и коннекторы к BigQuery/Redshift пришлось бы писать вручную;
- K8s CronJob: только расписание, пайплайна и зависимостей между шагами нет;
- Spark: движок вычислений, а не оркестратор; на 1 млн строк избыточен, но может быть вызван из Airflow.

### Интеграции

Для всех нужных систем есть готовые провайдеры, отдельный код писать не придётся:

| Система | Провайдер / оператор |
|---|---|
| PostgreSQL | `apache-airflow-providers-postgres` (`PostgresHook`, `SQLExecuteQueryOperator`) |
| BigQuery | `apache-airflow-providers-google` (`BigQueryInsertJobOperator`, `GCSToBigQueryOperator`) |
| Redshift | `apache-airflow-providers-amazon` (`RedshiftSQLOperator`, `S3ToRedshiftOperator`) |
| Kafka | `apache-airflow-providers-apache-kafka` (`ConsumeFromTopicOperator`, `ProduceToTopicOperator`) |
| Spark | `apache-airflow-providers-apache-spark` (`SparkSubmitOperator`) |

### Ветвление, триггеры, retry, email

- Ветвление и условия: `BranchPythonOperator`, `ShortCircuitOperator`, `trigger_rule` для объединения веток.
- Event-triggers: сенсоры (`FileSensor`, `S3KeySensor`, `GCSObjectExistenceSensor`, `SqlSensor`), deferrable-операторы, датасеты (запуск DAG по обновлению данных) и REST API для внешнего запуска.
- Retry: `retries`, `retry_delay`, `retry_exponential_backoff`, `execution_timeout` на уровне DAG или отдельной задачи.
- Fallback: `trigger_rule="one_failed"` позволяет запустить компенсирующую ветку, `on_failure_callback` отправляет уведомление.
- Email: `EmailOperator` и `email_on_failure` / `email_on_retry`, SMTP настраивается в конфигурации.

### Развёртывание в облаке

Локальный docker-compose переносится почти без изменений в логике DAG:
- Kubernetes (GKE / EKS): официальный Helm-чарт `apache-airflow/airflow` с `KubernetesExecutor` или `CeleryExecutor`, каждая задача выполняется в отдельном поде и масштабируется под нагрузку;
- метаданные Airflow хранятся в управляемом PostgreSQL (Cloud SQL / RDS) вместо контейнера;
- DAG'и подтягиваются из git (git-sync), логи уходят в GCS/S3;
- секреты и подключения (`shop_db`, BigQuery, Kafka) берутся из Secret Manager через secrets backend;
- альтернатива без самостоятельного администрирования: Cloud Composer (GCP) или MWAA (AWS), DAG'и остаются теми же.

## POC

Запуск:

```bash
docker compose up -d
```

- Airflow: http://localhost:8080 (admin / admin)
- Почта (MailHog): http://localhost:8025

DAG `delivery_report` (`dags/delivery_report.py`):

1. `read_deliveries`: читает CSV со статусами доставок (файловая система) и считает долю недоставленных;
2. `read_orders`: читает агрегаты по заказам из PostgreSQL (в таблице `orders` 1 млн строк, см. `init.sql`);
3. `choose_branch`: если доля недоставленных больше 5%, идёт `check_logistics` (письмо логистам), иначе `build_report` (запись отчёта в `delivery_report`);
4. `notify_success` / `notify_failure`: письмо об успехе или о падении пайплайна.

Retry: `retries=3`, задержка 10 секунд с экспоненциальным ростом. Чтобы его было видно, `read_orders` намеренно падает
на первой попытке и проходит на второй.

В тестовом CSV около 6% недоставленных, поэтому по умолчанию срабатывает ветка `check_logistics`.
Чтобы увидеть вторую ветку, поднимите `FAILED_THRESHOLD` в DAG выше этого значения.

Скриншоты (`screenshots/`):
- `1-dag-graph.jpg`: граф DAG и результат запуска, `build_report` и `notify_failure` пропущены;
- `2-retry-read-orders.jpg`: у `read_orders` две попытки, первая упала, вторая успешна;
- `3-emails-mailhog.jpg`: письма в MailHog.
