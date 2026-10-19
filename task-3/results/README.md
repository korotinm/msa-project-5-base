# Задание 3. Distributed Scheduling с k8s CronJob

Ежедневная выгрузка данных в 20:00 для дашбордов аналитиков. Для POC выгружается одна таблица `shipments`
(100 тыс. строк, нижняя граница из условия) из PostgreSQL в CSV.

## Состав

| Файл | Назначение |
|---|---|
| `export.py` | Читает таблицу через `COPY ... TO STDOUT WITH CSV HEADER` (потоково, без загрузки в память) и пишет `shipments_YYYYMMDD.csv` |
| `Dockerfile` | Образ `shipments-export:1.0`: python:3.12-slim + psycopg2 |
| `k8s/postgres.yaml` | Тестовая БД: Secret, ConfigMap со схемой и данными, Deployment, Service |
| `k8s/cronjob.yaml` | PVC для результатов и CronJob `shipments-export` с расписанием `0 20 * * *` |
| `k8s/viewer.yaml` | Вспомогательный под, чтобы посмотреть файлы на томе |

Параметры CronJob: `concurrencyPolicy: Forbid` (второй запуск не стартует поверх первого), `backoffLimit: 3`,
история 3 успешных и 3 неуспешных Job, requests/limits по ресурсам. Реквизиты БД берутся из Secret.

## Запуск в minikube

```bash
minikube start
eval $(minikube docker-env)
docker build -t shipments-export:1.0 .

kubectl apply -f k8s/postgres.yaml
kubectl rollout status deployment/postgres
kubectl apply -f k8s/cronjob.yaml
```

Чтобы не ждать 20:00, создайте Job из CronJob вручную:

```bash
kubectl create job --from=cronjob/shipments-export export-manual
kubectl logs job/export-manual
```

Либо временно поменяйте расписание на ежеминутное:

```bash
kubectl patch cronjob shipments-export -p '{"spec":{"schedule":"*/1 * * * *"}}'
kubectl get jobs -w
```

Результат на томе:

```bash
kubectl apply -f k8s/viewer.yaml
kubectl exec viewer -- ls -la /data
kubectl exec viewer -- head -5 /data/shipments_$(date +%Y%m%d).csv
```

Очистка: `kubectl delete -f k8s/ && kubectl delete job export-manual`.

## Istio

В namespace `default` включена инжекция Istio. Sidecar `istio-proxy` не завершается после основного контейнера,
и Job остаётся в `NotReady` навсегда. Поэтому в pod template CronJob стоит аннотация `sidecar.istio.io/inject: "false"`.

## Результат

В `screenshots/` лежит вывод `kubectl` (оформлен как скриншоты терминала):

- `1-cronjob-jobs-pods.jpg`: CronJob с расписанием `0 20 * * *`, три Job со статусом `Complete 1/1`, поды `Completed`;
- `2-job-logs-cronjob.jpg`: лог Job (`exported 100000 rows ...`) и параметры CronJob (`Concurrency Policy: Forbid`);
- `3-export-result.jpg`: CSV на томе, 100 000 строк плюс заголовок.

Для демонстрации расписание временно ставилось на `*/1 * * * *`, два Job (`...402`, `...403`) создал сам CronJob;
после этого манифест применён заново с боевым расписанием.
