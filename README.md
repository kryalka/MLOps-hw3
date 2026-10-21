# MLOps-hw3

Скоринг транзакций из ДЗ №1 и переобучение CatBoost в Airflow на CPU.

## Запуск

Нужны Docker с Compose V2 и 8 ГБ памяти. Команды выполняются из корня проекта.
Положите **train.csv** с колонкой `target` из
[Kaggle](https://www.kaggle.com/competitions/teta-ml-1-2025/data) в `data/train.csv`.

```bash
docker compose up -d --build --wait --wait-timeout 600
docker compose ps -a
```

`postgres`, `kafka`, `airflow` должны стать `healthy`, а `data_loader`
и `kafka-setup` — завершиться с кодом 0. Настройка `.env` не требуется.

| Сервис | Адрес |
|---|---|
| Airflow | http://localhost:8081 — `admin` / `admin` |
| Kafka UI | http://localhost:8080 |
| DQ-отчёт | http://localhost:8082/dq/latest.html |
| Метрики моделей | http://localhost:8082/models/ |
| Интерфейс скоринга | http://localhost:8501 |

## Проверка

Все DAG запускаются каждые **5 минут**, `catchup=False`.
Вручную: Airflow → нужный DAG → **Trigger DAG**.

1. Запустите `retrain_catboost`. В **Graph** все пять задач должны завершиться
   успешно. После первой публикации появляется `model_YYYYMM.cbm`.
   Первый прогон может уже выполниться по расписанию.
2. Запустите `supply_labeled_batch`, затем снова `retrain_catboost` —
   добавится следующий месяц и сдвинется окно обучения.
3. Запустите `data_quality` и откройте DQ-отчёт: ожидается **PASS**.
   При нарушениях отчёт содержит **FAIL**, последняя задача падает.

Проверьте файл модели и результаты скоринга:

```bash
docker compose exec airflow ls -lh /shared/models
docker compose exec airflow cat /shared/models/latest.json
docker compose exec postgres psql -U fraud_user -d fraud_db -c \
  "SELECT transaction_id, score, fraud_flag, model_version FROM scores ORDER BY id DESC LIMIT 10;"
```

Producer отправляет транзакции автоматически. У новых строк `model_version`
должен совпадать с `version` из `latest.json`. Модель обновляется без рестарта.
Модели лежат в `/shared/models/`, отчёты — в соседнем `/shared/reports/`.
Логи ошибки: Airflow → задача → **Logs** или `docker compose logs --tail=50`.

## Обучение и публикация

Окно — **3 месяца**: первые **2** для train, последний для test.
При сдвиге старейший месяц исключается. Supply копирует **5 000 строк**
(или всю витрину, если строк меньше) в следующий календарный месяц.
Глубина истории — **36 месяцев**.

Препроцессинг из ДЗ №1 дополнен `amount_log1p`, `hour_sin`, `hour_cos`, `is_night`.
Кандидат и активная модель сравниваются на одном test-месяце по **PR-AUC**
(average precision) и **ROC-AUC**. Публикация разрешена, если обе метрики
не ниже текущих. Иначе `approved=false`, активная версия сохраняется.

Повтор использует сохранённые месяц и решение. Указатель меняется только
после сохранения кандидата, включая отклонённого; повтор не сдвигает его снова.

## Data Quality

Проверяется вся витрина. HTML сохраняется в `/shared/reports/dq/`.

- Выборка не пустая, все исходные колонки и `row_id` присутствуют.
- Обязательные поля заполнены, `row_id` уникален.
- `transaction_time` корректен, `target` равен 0 или 1.
- `amount` — конечное положительное число.
- `population_city` — конечное неотрицательное число.
- Широты в [-90,90], долготы в [-180,180].

Остановка с сохранением данных: `docker compose down`.
Полный сброс данных и моделей: `docker compose down --volumes --remove-orphans`.
