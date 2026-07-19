# Аудит и восстановление порядка Git — 2026-07-19

## Итог

Рабочее дерево, в котором смешались изменения с 13 июня по 19 июля 2026 года, разобрано на логические блоки и зафиксировано. Runtime-состояние, snapshot-копии и резервные архивы больше не отслеживаются. Локальные файлы приложения и архивы сохранены физически. Ветка не отправлялась в remote и история опубликованных коммитов не переписывалась.

## Что произошло

- Последним коммитом перед накопившейся работой был `01b3703 Implement unified live auto strategies` от 2026-06-13.
- Локальная `main` уже была на 11 коммитов впереди `origin/main` (`3314dc2`), но после `01b3703` новые задачи выполнялись без регулярной фиксации.
- К 2026-07-19 в одном рабочем дереве находились pump/dump research, shadow/paper monitor, manual/live execution, auto strategies, de-risk/protective logic, Android UI и runtime-состояние.
- Общие файлы `webapp/services.py`, `execution/manual.py`, `risk/stop_manager.py` и связанные тесты менялись несколькими задачами. Безопасно восстановить поминутные исторические границы задним числом было невозможно: искусственное разрезание hunks могло создать нерабочие промежуточные состояния.
- Git отслеживал 76 путей `state/`, включая SQLite, auto-exit JSON и Telegram `.sent`-маркеры. Они менялись при обычной работе сервиса и постоянно загрязняли `git status`.
- Git также отслеживал 100 файлов полной копии проекта в `snapshots/` и 5 ZIP-бэкапов. Snapshot содержал дубликаты тестовых модулей и ломал обычный root `pytest` ошибкой `import file mismatch`.

## Как изменения разделены

1. `daa35cc Establish Git discipline and ignore runtime state`
   - добавлена обязательная Git-инструкция;
   - `state/`, `backups/`, `tmp/` и `.bundle` исключены из будущего отслеживания;
   - 76 runtime-путей удалены только из индекса.
2. `9440a4a Add pump lifecycle research and shadow monitor`
   - 103 файла: Bybit collectors, pump short/long research, portfolio reports, CLI, тесты, shadow/paper controller и pump UI;
   - 36 413 добавленных строк;
   - перед фиксацией: 74 pump/bybit теста.
3. `5bb893e Consolidate live execution and risk safeguards`
   - 48 файлов: manual execution, auto-enter/exit, Grid, account monitor, MEXC handling, WS reconciliation, storage, emergency de-risk, protective orders, FastAPI и web UI;
   - включает verified-position trust и безопасную очистку stale auto-exit/orphan protective orders;
   - добавлен `pytest.ini`, чтобы root `pytest` собирал только `tests/`;
   - 10 374 добавления и 1 906 удалений.
4. `381f495 Improve mobile execution controls`
   - 7 Android/документационных файлов;
   - async execution feedback, position actions, spread controls и expandable execution timing;
   - обычное время `1..30` минут и `Until filled` с пределом 30 минут.
5. `12eae77 Stop tracking local snapshots and backup archives`
   - 100 snapshot-файлов и 5 ZIP-бэкапов удалены только из индекса;
   - локальные копии подтверждены как сохранённые;
   - `snapshots/` добавлен в `.gitignore`.

## Проверки

- Компиляция всех изменённых Python-файлов: успешно.
- Полный root suite после добавления `pytest.ini`: `465 passed`, только deprecation warnings FastAPI/aiohttp.
- Отдельный research suite: `74 passed`.
- Android: `:app:testDebugUnitTest` (`NO-SOURCE`) и `:app:assembleDebug` — `BUILD SUCCESSFUL`.
- Каждый новый блок проверен через staged stat, `git diff --cached --check`, scope guard и поиск добавленных secret literals.
- APK сохранён как локальный build artifact и не добавлен в Git.

## Важное ограничение

Удаление архивов и snapshot из текущего индекса не уменьшает старую историю Git: blobs остаются в прежних коммитах. Полное физическое уменьшение `.git` потребовало бы `git filter-repo`/переписывания истории и принудительной синхронизации remote. Это намеренно не выполнялось без отдельного решения пользователя.

## Правила на будущее

- Единственный автоматизированный Git-писатель — Codex, пока пользователь прямо не разрешит иное.
- Каждая независимая задача заканчивается тестами, staged review и отдельным логическим коммитом.
- Нельзя начинать следующую независимую задачу поверх уже завершённой незакоммиченной работы.
- Runtime, данные исследований, логи, базы, screenshots, сборки и локальные архивы не коммитятся.
- В PowerShell после нативных команд явно проверяется `$LASTEXITCODE`.
- Push и переписывание опубликованной истории выполняются только по прямой просьбе пользователя.
- Каноническая инструкция: `instructions/12_GIT_ПОРЯДОК_И_КОММИТЫ.md`.
