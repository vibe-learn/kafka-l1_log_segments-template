        # kafka — Лог-сегменты и индексы

        Homework-шаблон для урока **l1_log_segments** (Лог-сегменты и индексы) на платформе Vibe Learn.

        ## Что делать

        Реализуй Go-программу из двух частей:

**Часть 1 — Producer:** пишет 100 000 сообщений в топик `segment_test` с настройками
`segment.bytes=10MB` и `segment.ms=10000` (10 секунд). Каждое сообщение — JSON с полями
`id` (порядковый номер) и `ts` (Unix timestamp). Используй segmentio/kafka-go.

**Часть 2 — Inspector:** Go-скрипт обходит Docker volume с data-директорией брокера
(`/var/kafka-logs/segment_test-0/`), находит все `.log` файлы, выводит их имена и размеры,
считает общее число сегментов.

**CI assertions (встроены в тест в шаблоне):**
- Число сегментов > 5 (подтверждает что rollover произошёл хотя бы 5 раз).
- Все сегменты кроме последнего имеют размер ≤ 10 MB + 10% (допуск на overhead).
- Имена файлов соответствуют формату `<20-digit-base-offset>.log`.

Требования к коду:
- Корректный graceful shutdown на SIGINT.
- Логирование partition+offset для каждого 1000-го ack.
- Обработка ошибок: retry до 3 раз с экспоненциальным backoff.

## Контекст (из transfer-задачи урока)

У вас топик `audit_log` пишет **10 KB/s**, `retention.ms=7776000000` (90 дней), `segment.bytes=1GiB`.
После года в production оператор замечает две вещи:
(1) Disk usage растёт линейно — примерно 864 MB в месяц (ожидаемо).
(2) Новые сегменты **ОЧЕНЬ редко закрываются**: директория партиции содержит один-два
    .log файла, хотя топик активен уже год.

**Вопрос:**

## Recap из урока

- **Партиция = упорядоченный набор сегментов.** Каждый сегмент — семейство файлов с общим base offset: `.log` (байты), `.index` (offset→position), `.timeindex` (timestamp→offset).
- **Ровно один active segment** принимает запись в каждый момент; все остальные — closed (immutable). Индексы memory-mapped в page cache.
- **Rollover** срабатывает по первому из двух условий: `segment.bytes` (default 1 GiB) или `segment.ms` (default 7 дней). Сегмент — единица retention: Kafka удаляет целые сегменты.
- **`.index`** — sparse mapping offset→position; поиск за O(log N) бинарным поиском, затем линейный scan. **Slow consumer** вытесняет страницы из page cache → disk read fallback вместо бесплатного RAM-чтения.
- **Anti-pattern:** `segment.bytes=1MB` → взрыв числа файловых дескрипторов и metadata overhead. Регулируй частоту ротации через `segment.ms`, а не через уменьшение `segment.bytes`.

        ## Как работать

        1. Платформа Vibe Learn создаёт копию этого репо в твоём GitHub-аккаунте по клику «Начать домашку» на странице урока (через GitHub `/generate`, codecrafters-pattern).
        2. Склонируй копию локально, реализуй TODO в `main.go`, прогони тесты, запушь.
        3. CI (`.github/workflows/ci.yml`) запускает `go vet` + `go test ./...` на каждый push. Платформа слушает результат через webhook от GitHub Actions и обновляет статус домашки на странице урока.

        ## Локальное окружение

        - Go 1.22+
        - Docker + docker-compose — `docker compose -f docker-compose.yml up -d` поднимает 3-нодовый Kafka cluster на портах 9092/9093/9094, использовать в тестах через bootstrap `localhost:9092,localhost:9093,localhost:9094`.

        ## Запуск

        ```bash
        # Поднять локальный Kafka
        docker compose up -d

        # Прогнать тесты (часть из них стартует свой ephemeral testcontainers cluster, часть использует docker-compose выше)
        go test ./...

        # Запустить main (печатает marker; замени stub на реализацию)
        go run .
        ```

        ## Заметка автора

        Это baseline-шаблон, сгенерированный платформой. Бизнес-сущность задачи (что конкретно реализовать в `main.go`, какие тесты сделать строгими) расширяется по ходу итераций — параллельно с углублением теории урока.
