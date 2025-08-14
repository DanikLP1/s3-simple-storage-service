# 🗄️ S3 Storage Service (Mini S3)

**Минималистичное S3-совместимое объектное хранилище** на Go.  
Поддерживает bucket'ы, версии объектов, lifecycle-политику и совместимо с `aws-cli`.

---

## ✨ Возможности
- 📦 **Bucket'ы и объекты** — создание, удаление, листинг.
- 🆕 **Версионность** — хранение нескольких версий одного ключа.
- 🗑 **Soft Delete** через DeleteMarker.
- 🔄 **Idempotency Keys** — защита от повторных загрузок.
- 🧹 **Lifecycle Worker** — автоматическая чистка:
  - устаревших версий
  - старых delete-marker'ов
  - мягко удалённых объектов
- 📁 **Дедупликация blob'ов** — по SHA256-хэшу.
- ⚡ **Совместимость с AWS CLI** (частично).

---

## 🛠 Архитектура
```mermaid
flowchart TD
    A[aws-cli / SDK] -->|HTTP S3 API| B[Mini S3 Server]
    B --> C[HTTP Router]
    C --> D[Handlers]
    D -->|метаданные| E[(SQLite / PostgreSQL)]
    D -->|байты файлов| F[Blob Storage (disk)]
    G[Lifecycle Worker] --> E
    G --> F
```
## 🚀 Быстрый старт ##
**1. Установка и запуск**
``` bash
git clone https://github.com/USERNAME/s3-storage-service.git
cd s3-storage-service
go run ./cmd/s3mini
```

По умолчанию сервер запускается на http://localhost:8080.

---

**2. Работа через AWS CLI**
```bash
#Создать bucket
aws --endpoint-url http://localhost:8080 s3 mb s3://test-bucket

#Загрузить файл
aws --endpoint-url http://localhost:8080 s3 cp file.txt s3://test-bucket/

#Список объектов
aws --endpoint-url http://localhost:8080 s3 ls s3://test-bucket

#Удалить объект (soft delete)
aws --endpoint-url http://localhost:8080 s3 rm s3://test-bucket/file.txt
```

---

## 🚀 Lifecycle-политики ##

**Пример: удалять старые версии через 30 дней** 

> lifecycle.json:

```json
{
  "Rules": [
    {
      "ID": "PurgeOldVersions",
      "Status": "Enabled",
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 30
      }
    }
  ]
}
```

**Применение:**

```bash
aws --endpoint-url http://localhost:8080 \
    s3api put-bucket-lifecycle-configuration \
    --bucket test-bucket \
    --lifecycle-configuration file://lifecycle.json
```

---
## 🔍 Параметры lifecycle ##
| Поле                            | Что делает                                             |
| ------------------------------- | ------------------------------------------------------ |
| `ExpireCurrentAfterDays`        | Удаляет текущую (HEAD) версию, если она старше N дней. |
| `ExpireNoncurrentAfterDays`     | Удаляет устаревшие версии, старше N дней.              |
| `NoncurrentNewerVersionsToKeep` | Хранит только N последних версий, остальные удаляет.   |
| `PurgeDeleteMarkersAfterDays`   | Удаляет delete-marker'ы старше N дней.                 |

---

## 🧩 Структура проекта 

```csharp
cmd/                # main.go и запуск сервера
internal/
  db/               # транзакции и модели
  server/           # HTTP-обработчики
  storage/          # драйвер хранения
  lifecycle/        # воркеры lifecycle
configs/            # конфиги
scripts/            # вспомогательные утилиты
```

---

## 🧪 Для разработчиков
```bash
Запуск тестов
go test ./...

Линт
golangci-lint run
```

**Makefile (пример)**
```Makefile
run:
	go run ./cmd/s3mini

test:
	go test ./...

lint:
	golangci-lint run
```

## 📅 Дорожная карта

 - Multipart Upload

 - Полный ListObjectsV2

 - Репликация между узлами

 - S3 Select

 - Перенос на PostgreSQL для кластера