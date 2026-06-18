## Airflow проекты

В этой папке находятся **два независимых тренировочных проекта** на Apache Airflow. Оба используют общий стек (Airflow + PostgreSQL + Docker).

---

### Проект 1. Автоматическая загрузка GitHub Events

**Кратко:** DAG загружает 100 последних публичных событий GitHub API каждые 5 минут, проверяет лимит запусков и логирует результат.

**Подробнее:** [README.md](./README.md)

---

### Проект 2. SCD Type 2 (Slowly Changing Dimension)

**Кратко:** DAG-и для инкрементальной загрузки данных из источника в staging и dimension-слой с полной историей изменений.

**Подробнее:** [README.md](./scd2_project/README.md)

---

#### Общий стек

- Apache Airflow (v2.9.3)
- PostgreSQL (метабаза и целевая БД)
- pgAdmin
- Docker & Docker Compose

#### Запуск

```bash
docker-compose up -d
```

После запуска:

Airflow UI: http://localhost:8080

pgAdmin: http://localhost:5050