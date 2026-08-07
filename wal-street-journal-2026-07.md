```
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║        W A L   S T R E E T   J O U R N A L                                   ║
║        ═══════════════════════════════════════                               ║
║        Ежемесячник хакеров PostgreSQL · Выпуск №1, том XX                    ║
║                                                                              ║
║        «Всё, что записано в журнал, — уже история»                           ║
║                                                                              ║
║   Июль 2026 · Первый коммитфест PostgreSQL 20 · master @ 20devel · ¤ 0 руб.  ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

---

# 🔨 ПЕРЕДОВИЦА: «Let the hacking begin ...»

> ```
> commit a281a3e6dbb45d5cca41ea5f7b746724eb3ca70d
> Author: Joe Conway
> Date:   Mon Jun 29 16:29:11 2026 -0400
>
>     Stamp HEAD as 20devel.
>
>     Let the hacking begin ...
> ```

Три слова и многоточие. Каждый год в конце июня один из коммиттеров печатает эту
фразу, и дерево `master` перестаёт быть версией N, становясь версией N+1. В этом
году честь выпала **Джо Конвею**, и вместе с бампом номера версии в `configure`,
`meson.build` и `version_stamp.pl` произошло главное ритуальное жертвоприношение
цикла:

```diff
 doc/src/sgml/release-19.sgml | 3330 ------------------------------------------
 doc/src/sgml/release-20.sgml |   16 +
```

**3330 строк** заметок о релизе 19 отправились в ветку `REL_19_STABLE`, а на их
месте появился пустой файл-заготовка на 16 строк. Он и стал холстом июля.

Уже через 40 минут после стампа в дерево пошли патчи. К моменту, когда часы
пробили 1 августа, заготовка перестала быть пустой.

---

# 📊 ЦИФРЫ НОМЕРА

```
   ┌────────────────────────────────────────────────────────────────────┐
   │  КОММИТОВ ПОСЛЕ «Let the hacking begin ...»            324         │
   │  из них строго в июле                                  302         │
   │  ДОБАВЛЕНО СТРОК                                  +24 656          │
   │  УДАЛЕНО СТРОК                                    −10 946          │
   │  ЧИСТЫЙ ПРИРОСТ                                   +13 710          │
   │  ЗАТРОНУТО ФАЙЛОВ                                      736         │
   │  КОММИТТЕРОВ У РУЛЯ                                     28         │
   │  ИМЁН В ТРЕЙЛЕРАХ (авторы + ревьюеры + репортёры)      182         │
   │  РЕВЕРТОВ                                                5         │
   │  КОММИТОВ, НАЧИНАЮЩИХСЯ С «Fix»                         74         │
   └────────────────────────────────────────────────────────────────────┘
```

**74 из 324.** Почти четверть июля ушла на починку того, что успели сломать в
июне. Первый коммитфест нового цикла — это всегда наполовину уборка после беты
предыдущего.

## Пульс месяца: коммитов по неделям

```
 нед. 27  (29.06–05.07) ████████████████████████████████████████████  74
 нед. 28  (06.07–12.07) █████████████████████████████████████████████████████  88  ← пик
 нед. 29  (13.07–19.07) ███████████████████████████████████████████  72
 нед. 30  (20.07–26.07) ████████████████████████  40  ← отпуска
 нед. 31  (27.07–31.07) ██████████████████████████████  50
                        └────┴────┴────┴────┴────┴────┴────┴────┴────┘
                        0   10   20   30   40   50   60   70   80   90
```

## Когда коммитят хакеры

```
  Пн  ██████████████████████████  42
  Вт  ████████████████████████████████████  59
  Ср  ██████████████████████████████████████████████████  82   ← «среда — день push»
  Чт  ███████████████████████████████████  58
  Пт  █████████████████████████████████████  61
  Сб  ████████  14
  Вс  █████  8
```

Выходные священны — 22 коммита из 324 (6.8%). Но среда — день, когда дерево
трясёт сильнее всего: 82 коммита, вчетверо больше, чем за оба выходных вместе.

## Топ коммиттеров июля

```
  Peter Eisentraut     ███████████████████████████████████████████████  45
  Michael Paquier      ████████████████████████████████████████  39
  Fujii Masao          ██████████████████████████████████████  36
  Tom Lane             ███████████████████████████████  30
  Heikki Linnakangas   ██████████████████  17
  Daniel Gustafsson    █████████████████  16
  Amit Kapila          █████████████████  16
  Alexander Korotkov   ███████████████  14
  Nathan Bossart       ██████████████  13
  Richard Guo          █████████████  12
  Masahiko Sawada      ████████████  11
  Álvaro Herrera       ███████████  10
  Jeff Davis           ███████████  10
  Peter Geoghegan      ██████████  9
  Melanie Plageman     █████████  8
  Robert Haas          ████████  7
  David Rowley         ███████  6
  ... и ещё 11 коммиттеров
```

## Куда легли изменения (число касаний файлов)

```
  src/test/            ███████████████████████████████████████████████  253
  src/bin/             ████████████████████████████  152
  contrib/             ███████████████████████  123
  src/backend/utils/   ███████████████████  101
  src/include/         ███████████████████  100
  doc/                 ██████████████████  97
  src/backend/access/  ███████████  57
  src/backend/commands ████████  41
  src/backend/replicat ███████  39
  src/backend/storage/ ██████  30
  src/interfaces/      █████  27
  src/backend/parser/  ████  23
  src/backend/optimize ████  19
```

Тестов правили больше, чем всего остального. Это хороший знак — и одновременно
портрет первого коммитфеста: фичи прошлого цикла обрастают покрытием.

---

# 🥇 ГЛАВНАЯ ПОЛОСА: JSON_TABLE PLAN — сага длиной в четыре года

**Коммит `86ab7f4c721`, Alexander Korotkov, 9 июля.**
`+2017 / −112`, 13 файлов, **семь** соавторов и **одиннадцать** ревьюеров.

Если у июля есть один флагманский патч — это он. Ссылки в `Discussion:` уходят
в 2022 год. `JSON_TABLE` приехал в PostgreSQL 17 без клаузы `PLAN`, и всё это
время висел долг перед стандартом SQL/JSON.

## Что решает PLAN

`JSON_TABLE` умеет разворачивать вложенные пути через `NESTED PATH`. Вопрос: как
склеивать строки от нескольких вложенных путей? До июля ответ был зашит наглухо:
родитель с детьми — `LEFT OUTER`, братья между собой — `UNION`. Теперь это
управляется.

```
                   ┌──────────────────────────────┐
                   │  row pattern  '$'  AS root   │
                   └──────────────┬───────────────┘
                                  │ INNER / OUTER   ← родитель↔ребёнок
                  ┌───────────────┴────────────────┐
                  │                                │
       ┌──────────▼──────────┐          ┌──────────▼──────────┐
       │ NESTED '$.phones'   │  UNION   │ NESTED '$.emails'   │
       │        AS ph        │◄────────►│        AS em        │
       └─────────────────────┘  CROSS   └─────────────────────┘
                                  ▲
                                  └─ братья: UNION (по строке на каждого)
                                     или CROSS (декартово произведение)
```

## Синтаксис

```sql
[ PLAN ( json_table_plan )
| PLAN DEFAULT ( { INNER | OUTER } [ , { CROSS | UNION } ]
               | { CROSS | UNION } [ , { INNER | OUTER } ] ) ]

json_table_plan is:
    json_path_name [ { OUTER | INNER } json_table_plan_primary ]
  | json_table_plan_primary { UNION json_table_plan_primary } [...]
  | json_table_plan_primary { CROSS json_table_plan_primary } [...]
```

Простая форма — `PLAN DEFAULT`, глобальная стратегия на весь вызов:

```sql
SELECT * FROM JSON_TABLE(
    jsonb '[]', '$'
    COLUMNS (
        foo int PATH '$'
    )
    PLAN DEFAULT (UNION)
) jt;
```

Полная форма — `PLAN (...)` — расписывает стратегию для каждого именованного
пути персонально.

## Постгресовое отступление от стандарта

Стандарт SQL/JSON требует: если есть `PLAN`, то **каждый** `NESTED PATH` обязан
иметь имя через `AS`. PostgreSQL решил иначе — имена необязательны, безымянным
путям имя генерируется автоматически:

```sql
-- Работает: PLAN DEFAULT с безымянным NESTED path
SELECT * FROM JSON_TABLE(
    jsonb '[]', '$' AS path1
    COLUMNS (
        NESTED PATH '$' COLUMNS ( foo int PATH '$' )
    )
    PLAN DEFAULT (UNION)
) jt;

-- А вот это упадёт: явный PLAN() ссылается на пути по имени,
-- а сгенерированное имя назвать невозможно
SELECT * FROM JSON_TABLE(
    jsonb '[]', '$' AS path1
    COLUMNS (
        NESTED PATH '$' COLUMNS ( foo int PATH '$' )   -- ← безымянный!
    )
    PLAN (path1)
) jt;
-- ERROR: nested path not covered by the plan
```

## Хроника после посадки

Большие патчи не приземляются чисто. За две недели после `86ab7f4c721`:

| Дата | Коммит | Что |
|------|--------|-----|
| 9 июля  | `Bump catversion for JSON_TABLE PLAN clause` | забыли catversion |
| ~10 июля | `Revert "Cascading of JSON_TABLE's ON ERROR"` | откат смежного поведения |
| ~10 июля | `Fix JSON_TABLE PLAN deparse to keep parentheses around nested joins` | `pg_get_viewdef` терял скобки |
| ~10 июля | `Make JSON_TABLE generated path names avoid collisions` | автоимена сталкивались |
| ~10 июля | `Fix and polish JSON_TABLE documentation` | |
| ~10 июля | `Avoid redundant re-evaluation of JSON_TABLE nested paths` | производительность |
| ~10 июля | `Remove unreachable error check in JSON_TABLE plan transform` | |
| ~25 июля | `Fix deparsing of JSON_ARRAY(subquery) with a FORMAT clause` | рикошет |

Итого: 12 коммитов месяца с «JSON» в заголовке. Классическая кривая посадки
большой фичи.

---

# 🥈 ВТОРАЯ ПОЛОСА: конфликты логической репликации — теперь в таблице

**Коммит `a5918fddf10`, Amit Kapila, 2 июля.** 31 файл, `+1451 / −40`.

Раньше конфликт логической репликации отправлялся в `postgresql.log` простым
текстом. Хочешь проанализировать — парси логи регулярками. Теперь конфликты
можно писать в таблицу.

```sql
CREATE SUBSCRIPTION sub
  CONNECTION 'host=pub dbname=app'
  PUBLICATION pub
  WITH (conflict_log_destination = 'table');   -- 'log' | 'table' | 'all'

ALTER SUBSCRIPTION sub SET (conflict_log_destination = 'all');
```

## Архитектура

```
    ┌───────────────────────────────────────────────────────────────┐
    │                     ПОДПИСЧИК                                 │
    │                                                               │
    │   apply worker                                                │
    │        │                                                      │
    │        │  обнаружен конфликт                                  │
    │        │  (insert_exists / update_origin_differs / ...)        │
    │        ▼                                                      │
    │   ┌────────────────────────────┐                              │
    │   │  conflict_log_destination  │                              │
    │   └──────┬──────────────┬──────┘                              │
    │      'log'│              │'table'                             │
    │          ▼              ▼                                     │
    │   postgresql.log   схема pg_conflict (системная)              │
    │                    └── pg_conflict_log_<subid>                │
    │                          ▲                                    │
    │                          │ internal dependency                │
    │                    DROP SUBSCRIPTION → таблица исчезает       │
    └───────────────────────────────────────────────────────────────┘
```

## Схема журнала конфликтов

Из `ConflictLogSchema[]` в исходниках:

```c
static const ConflictLogColumnDef ConflictLogSchema[] = {
    {.attname = "relid",                 .atttypid = OIDOID},
    {.attname = "schemaname",            .atttypid = TEXTOID},
    {.attname = "relname",               .atttypid = TEXTOID},
    {.attname = "conflict_type",         .atttypid = TEXTOID},
    {.attname = "remote_xid",            .atttypid = XIDOID},
    {.attname = "remote_commit_lsn",     .atttypid = LSNOID},
    {.attname = "remote_commit_ts",      .atttypid = TIMESTAMPTZOID},
    {.attname = "remote_origin",         .atttypid = TEXTOID},
    {.attname = "replica_identity_full", .atttypid = BOOLOID},
    {.attname = "replica_identity",      .atttypid = JSONOID},
    {.attname = "remote_tuple",          .atttypid = JSONOID},
    {.attname = "local_conflicts",       .atttypid = JSONARRAYOID}
};
```

**Обратите внимание на `json`, а не `jsonb`.** Комментарий в коде объясняет
выбор прямо: это *точный аудиторский снимок*, а `jsonb` его нормализовал бы —
переупорядочил ключи, схлопнул дубликаты, потерял форматирование чисел. Искать
по кортежам никто не собирается; индексируют скалярные колонки (`relid`,
`conflict_type`, timestamp), а json-поля — это payload для разбора постфактум.

## Таблица под замком

Таблица системно-управляемая, и защищена агрессивно. **Запрещено:** `ALTER`,
`DROP`, `CREATE INDEX`, триггеры, правила, политики, расширенная статистика,
наследование, использование как FK-цель, а также `INSERT`, `UPDATE`, `MERGE` и
блокировка строк вручную.

**Разрешено ровно два действия:** `DELETE` и `TRUNCATE` — чтобы можно было
чистить старьё.

Плюс: таблицы конфликтов исключены из публикаций, даже из `FOR ALL TABLES`.
Иначе — весёлый бесконечный цикл.

---

# 📈 СТАТИСТИКА: месяц Мишеля Паке

Из 39 коммитов Michael Paquier львиная доля — про `pgstat`. 25 коммитов месяца
так или иначе трогают статистику.

## Блокировки, теперь по бэкендам

**`8c579bdc366`, Bertrand Drouvot / Michael Paquier, 30 июня.**

Была `pg_stat_lock` — общая по кластеру. Стала ещё и персональная:

```sql
-- одна строка на каждый тип блокировки для конкретного PID
SELECT * FROM pg_stat_get_backend_lock(12345);
```

Сигнатура из `pg_proc.dat`:

```
proname     => 'pg_stat_get_backend_lock',
proargtypes => 'int4',
proargnames => '{backend_pid, locktype, waits, wait_time,
                 fastpath_exceeded, stats_reset}'
```

Живая картина по всем работающим бэкендам — джойном с `pg_stat_activity`:

```sql
SELECT a.pid, a.state, a.query, l.locktype, l.waits, l.wait_time,
       l.fastpath_exceeded
FROM   pg_stat_activity a
CROSS JOIN LATERAL pg_stat_get_backend_lock(a.pid) l
WHERE  l.waits > 0
ORDER BY l.wait_time DESC;
```

`fastpath_exceeded` — сколько раз бэкенд вылетел за пределы fast-path
блокировок. В том же месяце Паке дописал в документацию **целый новый раздел
про fast-path locking** (`2d31da52716`) — читать вместе.

Ещё одна деталь для перфекционистов: `stat_lock.wait_time` в течение месяца
дважды меняли тип — сначала на `double precision` (`c776550e466`), затем
чинили потерю точности в `pg_stat_us_to_ms()` (`Fix loss of precision`), потом
там же ловили опечатку. Микросекунды в миллисекунды — задача, оказавшаяся
сложнее, чем выглядит.

## Каталог видов статистики

**`3b066de6c0a`, Michael Paquier, 2 июля.**

```sql
CREATE VIEW pg_stat_kind_info AS
    SELECT k.id, k.name, k.builtin, k.fixed_amount,
           k.accessed_across_databases, k.write_to_file, k.entry_count
    FROM pg_stat_get_kind_info() k;
```

Аналог `pg_get_loaded_modules()`, но для видов статистики. Особенно полезно,
когда расширение регистрирует **кастомный** вид статистики через
`shared_preload_libraries` — теперь видно, что именно оно зарегистрировало и
сколько там записей.

## Ограничитель длины логируемых запросов

**`c8bd8387c27`, Fujii Masao, 3 июля.**

Проблема, знакомая любому DBA: приложение шлёт `INSERT` с литералом на 40 МБ,
включён `log_min_duration_statement`, и логи растут как на дрожжах.

```sql
-- обрезать тело логируемого запроса до 1 КБ
SET log_statement_max_length = 1024;

-- 0  — логировать пустое тело запроса
-- -1 — прежнее поведение (по умолчанию): логировать целиком
```

Действует на `log_statement`, `log_min_duration_statement`,
`log_min_duration_sample` и `log_transaction_sample_rate`. Обрезка байтовая
(единица измерения GUC — байты), но режет **по границам многобайтных
символов** — чтобы в логе не оказалось битого UTF-8.

Не влияет на `log_min_error_statement` — это отдельная история, оставленная на
потом. Патчу потребовалось ещё три follow-up коммита: чинили тест при verbose
логах, улучшали отчётность об усечении, упрощали вызовы `truncate_query_log()`.

---

# 🧭 ПЛАНИРОВЩИК

## enable_groupagg — недостающий выключатель

**`e01b23b84e4`, Richard Guo, 9 июля.**

У нас двадцать лет был `enable_hashagg`, а симметричного тумблера для
сортированной группировки не было. Теперь есть:

```sql
SET enable_groupagg = off;   -- по умолчанию on
```

Накрывает: `GroupAggregate`, `Group`, сортировочный `Unique` (для `DISTINCT` и
уникализации полусоединений) и сортированный режим `SetOp`.

**Это не жёсткий запрет** — как и другие `enable_*` в современном Postgres, он
лишь увеличивает `disabled_nodes`. Если альтернативы нет, план всё равно
построится.

Побочный эффект, который стоит отдельного упоминания: в регрессионных тестах
куча мест выключала `enable_sort` (а в одном — даже `enable_indexscan`) только
ради того, чтобы вынудить хешированный план. Теперь там честный
`enable_groupagg = off`. И в `union.sql` это впервые открыло дорогу к
тестированию **хешированного UNION** — до июля этот путь в тестах был просто
недостижим.

## Хеш-джойн и NULL: тихая утечка производительности

**`60826a352d4`, David Rowley, 31 июля.**

Детективная история на три коммита. `adf97c156` научил вычисление выражений
хешировать. `9ca67658d` починил там затирание памяти. И вот в этой починке
осталась щель.

```
  Хеширование нескольких ключей, strict-режим:

  ключ1 ──► EEOP_HASHDATUM_FIRST_STRICT ──► промежуточный хеш
                    │
                    │ ключ = NULL → jump to jumpdone
                    ▼
              ┌─────────────────┐
              │   jumpdone      │  ожидает: ExprState->resnull / resvalue
              └─────────────────┘  реальность: НЕ ЗАПОЛНЕНЫ ✗
                    │
                    ▼
        в хеш-таблицу летит запись, которая
        никогда ни с чем не сматчится
```

`op->resnull`/`op->resvalue` у нефинальных шагов указывают на ячейку
*промежуточного* хеша. Для не-NULL это правильно — значение бит-ротируется и
хеширование продолжается. Но при раннем выходе по NULL прыжок уходил на
`jumpdone`, а поля `ExprState` так и оставались незаполненными.

Результат: в хеш-таблицу вставлялись строки, которые **гарантированно** не
найдут партнёра по джойну. Не неправильный ответ — просто сожжённые CPU и
память.

Починка: `EEOP_HASHDATUM_FIRST_STRICT` и `EEOP_HASHDATUM_NEXT32_STRICT` теперь
заполняют `resnull`/`resvalue` в `ExprState` напрямую, когда значение NULL.

Hash Agg и хешированные подпланы не пострадали — они не используют STRICT-шаги.

## Прочее в оптимизаторе

- **`be69a5ff1fd`** — улучшена оценка числа строк на выходе `UNION` (Richard Guo)
- **`Propagate stadistinct through GROUP BY/DISTINCT in subqueries and CTEs`** —
  статистика уникальности теперь протекает сквозь подзапросы и CTE
- Трилогия про `nullingrels`: точные совпадения для `NestLoopParams`, удаление
  параметра `nrm_match` из `fix_upper_expr`, ужесточение проверок для внешних
  соединений
- **`Fix qual pushdown past grouping with mismatched equivalence`**
- **`Fix issue with RANGE's DEFAULT partition pruning`**
- **`Fix planner's nullability/strictness logic for ScalarArrayOpExpr`** (Tom Lane)
- **`Fix LIKE/regex optimization for indexscan with exact-match pattern`**
- **`Fix like_fixed_prefix_ci() selectivity`**
- **`Skip unnecessary get_relids_in_jointree() when there are no PHVs`**
- **`Fix incorrect Result node flattening logic`**

---

# 💀 НЕКРОЛОГ: GROUP BY ALL, 2026–2026

**`a32733d8f10`, Tom Lane, 17 июля.**

```
    ef38a4d9756  «Add GROUP BY ALL»              ← жизнь
    2ce745836    «comment improvements»
         ...
    a32733d8f10  Revert "Add GROUP BY ALL"       ← смерть
```

Фича, позволявшая писать `GROUP BY ALL` вместо перечисления неагрегатных
выражений, прожила один цикл и была откачена **из ветки 19 тоже**
(`Backpatch-through: 19`).

Причина, из коммит-сообщения:

> A postcommit review discovered that GROUP BY ALL missed our special handling
> of entries that also appear in an ORDER BY in the query. This caused the query
> to return **wrong results** when ORDER BY specifies non-default equality
> semantics. While this should be fixable with some refactoring, doing it
> cleanly seems like too much code churn for late beta. We'll revert and try
> again in v20.

Классический постгресовый расклад: неправильные результаты + поздняя бета =
откат, без обсуждений. Требуется `catversion bump` — менялась структура `Query`.
Улучшения комментариев и документации из того же патча оставили.

Репортёр — Chao Li. Обещание «We'll revert and try again in v20» — на столе.
Комитфест сентября покажет.

Строчкой ниже в июльском логе: `Update GROUP BY ALL comments about window
functions` — коммит от 8 июля, попавший в дерево за девять дней до похорон.

---

# 🧹 ВЕЛИКАЯ ЧИСТКА ИЮЛЯ

Первый коммитфест цикла — традиционно время выносить мусор. В этом июле вынесли
особенно много.

## Прощай, всё, что старше v10

**`3a0a30884fd` и соседи, Nathan Bossart, 2 июля.** Четыре коммита подряд:

```
  Remove pg_dump/pg_dumpall support for dumping from pre-v10 servers.
  Remove pg_upgrade support for upgrading from pre-v10 servers.
  Remove psql support for pre-v10 servers.
  Run pgindent and pgperltidy for previous 3 commits.
```

Плюс в конце месяца — `Remove code for pre-v10 servers from AdjustUpgrade.pm`.

Политика проекта: поддерживать минимум 10 предыдущих мажорных версий. Прошлый
раз планку двигали в 2021 году, до 9.2, для версии 15.

```
   Поддержка старых серверов в pg_dump/pg_upgrade/psql:

   2021, для v15:   9.2 ────────────────────────────────► 15
   2026, для v20:                       10 ─────────────► 20
                                        ▲
                                  новая нижняя граница
```

Способность `pg_restore` читать старые архивные файлы **не тронута** — хотя, как
честно замечено в коммит-сообщении, «fair to wonder how that might be tested
nowadays».

Отдельный штрих: чтобы диффы читались, Натан намеренно **не** запускал pgindent
внутри самих патчей — а отдельным коммитом после, и добавил его в
`.git-blame-ignore-revs`. В июле такое проделали трижды (`d69fdf79b8`,
`fdad19e1cf`, и ещё пара в конце месяца). Уважение к `git blame` — отдельный
жанр коммиттерской культуры.

## refint: сорок лет спустя

**`5e90e0914cf`, Nathan Bossart, 8 июля.**

> refint was sample code from the pre-built-in-FK era and has long been
> documented as superseded by the built-in foreign key mechanism. Recent fixes
> made it clear that the code has more issues than its sample-code value
> justifies.

В коммит-сообщении перечислены **шесть** предшествующих коммитов-починок:
`b0b6196386`, `8cfbdf8f4d`, `260e97733b`, `611756948e`, `1fbe2066dc`,
`1541d91d1c`. После шести заплаток на учебном примере из эпохи до
`FOREIGN KEY` — вердикт: удалить.

## Остальные жертвы

| Что удалено | Коммит | Комментарий |
|---|---|---|
| `pg_spin_delay()` | Nathan Bossart | больше не нужен |
| RADIUS из методов аутентификации initdb | Thomas Munro | |
| SQL-функция `getpgusername()` | | реликт |
| `WaitEventCustomCounterData` | | |
| `TerminateThread()` на Ctrl-C в Windows | | «sketchy», по авторской формулировке |
| «support» для `SECURITY LABEL ON PROPERTY GRAPH` | | его на самом деле не было |
| Логика encoding-aware truncation в btree_gist | Tom Lane | «useless» |
| Упоминание advice про слоты в подсказках MultiXact wraparound | | |

## Планка тулчейна поднята

Три коммита 16 июля, все — Peter Eisentraut:

```
  Raise requirement to Visual Studio 2022     ← прощай, VS 2019
  Drop support for _MSC_VER less than 1933
  Require ICU 55 or later                     ← прощай, RHEL 7
  Make PL/Tcl require Tcl 8.6 or later        ← а вот это откатили
```

Причина для MSVC вышла изящной: коммит `Replace __builtin_types_compatible_p
with _Generic` заставил VS 2019 выдавать ошибки компиляции. Компилятор
формально поддерживает `_Generic`, но, цитата, «is just broken for that». Вместо
обхода — поднять требование. Заодно в документацию добавили информацию про
VS 2026.

С ICU логика прямее: RHEL 7 больше не поддерживается, значит древние версии ICU
можно не тащить. Удаление открыло путь к заметной чистке кода — включая блоки,
которые «probably received very little actual testing and use».

Требование Tcl 8.6 продержалось меньше суток и было откачено
(`Revert "Make PL/Tcl require Tcl 8.6 or later"`).

---

# 🔬 ТИПОВАЯ ГИГИЕНА: 45 коммитов Питера Айзентраута

Абсолютный чемпион месяца по количеству коммитов. Тема — типы возвращаемых
значений системных вызовов, `const`-корректность и переход на современный C.

Прочитайте эти заголовки подряд — это цельная симфония:

```
  Clean up read() return type
  Clean up write() return type
  Clean up secure_read()/secure_write() return type
  Clean up readlink() return type
  Clean up copy_file_range() return type
  Make blkreftable API use size_t/ssize_t consistently
  Move Windows ssize_t definition earlier
  Add assertion about ssize_t narrowing in AIO code
  Don't cast off_t to 32-bit type for output, bug fix
  Don't cast pgoff_t to possibly 32-bit types for output
  Print off_t/pgoff_t consistently as %lld
  Fix more Datum conversion inconsistencies
  Some const qualifications added in passing
  Make SPI_prepare argtypes argument const
  Fixes for SPI "const Datum *" use
  Fix for loop variables
  Fix for loop variables used with lengthof
```

И переход на современный стандарт:

```
  Use C11 alignas instead of pg_attribute_aligned
  Replace __builtin_types_compatible_p with _Generic     ← это и убило VS 2019
  Shorten pg_attribute_always_inline to pg_always_inline
```

Отдельно стоит `Don't cast off_t to 32-bit type for output, bug fix` — это не
косметика, а реальный баг с файлами >2 ГБ на платформах, где `off_t` шире
`long`. Гигиена типов иногда оказывается ловлей багов.

---

# 🌳 ТОМ ЛЕЙН РАСКОПАЛ btree_gist

Серия из ~8 коммитов за 3–7 июля. Началось невинно:

```
  btree_gist: fix NaN handling in float4/float8 opclasses.
```

А закончилось так:

```
  Use the proper comparator in gbt_bit_ssup_cmp.
  Reverse-engineer some documentation for btree_gist's varlena modules.
  Fix btree_gist's NotEqual strategy on internal index pages.
  Sync signatures of gbt_var_consistent() and gbt_num_consistent().
  Tighten up btree_gist's handling of truncated bounds.
  Remove btree_gist's useless logic for encoding-aware truncation.
```

Заголовок **`Reverse-engineer some documentation for btree_gist's varlena
modules`** заслуживает отдельной рамки. Расширению больше двадцати лет,
документации на внутренности varlena-модулей не было, и её пришлось
восстанавливать по коду — прежде чем стало возможно чинить `NotEqual` на
внутренних страницах индекса.

Стратегия `NotEqual` на internal-страницах — это баг, дающий **неверные
результаты поиска**: на внутренней странице `<>` требует особой логики, потому
что диапазон может содержать и совпадающие, и несовпадающие значения.

---

# 🩺 СТАБИЛИЗАЦИЯ ФИЧ PostgreSQL 19

Второй по объёму пласт июля — доводка того, что село в прошлый цикл.

## Синхронизация последовательностей (13 коммитов)

Самый нервный кластер месяца. Логическая репликация последовательностей —
свежая фича, и июль её пробовал на прочность со всех сторон:

```
  Reject concurrent sequence refreshes.              ← 25 июля
  Revert "Reject concurrent sequence refreshes".     ← через несколько часов
  Handle concurrent sequence refreshes.              ← 27 июля, версия 2.0
```

Полный список болячек:

- гонки при конкурентных `REFRESH`
- `DROP` последовательности прямо во время синхронизации
- права: последовательности издателя с отказом в доступе показывались как
  «отсутствующие» (`Avoid reporting permission-denied publisher sequences as
  missing`)
- накопление блокировок отношений во время синхронизации
- отказ синхронизироваться с издателями старше PostgreSQL 19
- формулировки предупреждений про origin
- документация: поведение `pg_get_sequence_data()`, NULL-случаи, привилегии

## Онлайн-включение контрольных сумм (7 коммитов)

- инициализация счётчика прогресса
- обработка временных отношений и удалённых баз
- **`Handle invalid and dropped databases during checksum enable`**
- **`Recheck checksum state before file_copy during CREATE DATABASE`** — гонка
  между включением контрольных сумм и `CREATE DATABASE ... STRATEGY file_copy`
- ограничение записей `pg_stat_io` для процессов контрольных сумм
- документация прогресс-репортинга

## REPACK CONCURRENTLY (6 коммитов)

- `Fix REPACK CONCURRENTLY for stored generated columns`
- `REPACK CONCURRENTLY: Initialize the range table more honestly`
- `Don't lock tables in get_tables_to_repack()`
- `Move code to get_tables_to_repack_partitioned`
- `Fix LSN format in REPACK worker debug message`
- `doc: Mention REPACK in MAINTAIN privilege descriptions`

## FOR PORTION OF — темпоральные таблицы (6 коммитов)

Álvaro Herrera и Peter Eisentraut методично выбивали углы:

```
  Forbid generated columns in FOR PORTION OF
  Forbid FOR PORTION OF on views with INSTEAD OF triggers
  Fix RLS checks for FOR PORTION OF leftover rows
  Avoid RETURNING side effects for FOR PORTION OF leftovers.
  Test what BEFORE UPDATE triggers do to FOR PORTION OF
  Deparse FOR PORTION OF using the range column's current name.
```

«Leftover rows» — те самые обрезки диапазона, которые `FOR PORTION OF`
дописывает обратно в таблицу. Оказалось, что они не должны ни попадать в
`RETURNING`, ни проверяться политиками RLS как обычные строки.

## Property graph / SQL/PGQ (9 коммитов)

Свежий `GRAPH_TABLE` показывает все признаки годовалого кода:

```
  Prevent dropping the last label from a property graph element
  Fix handling of dropping a property not associated with the given label
  Fix properties orphaned by dropping a label
  Resolve unknown-type literals in GRAPH_TABLE COLUMNS
  Resolve unknown-type literals in property expressions
  Fix replace_property_refs() ignoring the root of expression tree
  Make property graph object descriptions better translatable
  Prohibit locking clauses on GRAPH_TABLE
  Fix pg_dump ACL minimization for PROPERTY GRAPH.
  Remove apparent support for SECURITY LABEL ON PROPERTY GRAPH
```

## Динамическое логическое декодирование (6 коммитов)

```
  Correct logical decoding status at end of recovery with minimal WAL level.
  pg_controldata: Show logical decoding status.
  Add logical decoding status to pg_control_checkpoint().
  Fix race condition when enabling logical decoding concurrently.
  Fix races between deactivation of logical decoding and slot creation.
```

Статус логического декодирования хранился в чекпойнт-записях, использовался при
старте — но `pg_controldata` его не показывал. Теперь показывает, и это
бэкпортировано в 19.

---

# 🚨 КРИМИНАЛЬНАЯ ХРОНИКА

Самые неприятные баги, пойманные за месяц.

**`8e684ce11dd` — Fix unlogged sequence corruption after standby promotion.**
Fujii Masao, 30 июня. Первый рабочий день цикла — и сразу порча данных.

**`8d85cb889a3` — bufmgr: Fix race in LockBufferForCleanup().**
Гонка в самом сердце менеджера буферов.

**`Fix RETURNING OLD with BEFORE UPDATE trigger and concurrent update.`**
Тройное совпадение: `RETURNING OLD` + BEFORE-триггер + конкурентный апдейт.

**`Prevent access to other sessions' empty temp tables.`**
Утечка изоляции между сессиями.

**`Prevent satisfies_hash_partition from crashing with VARIADIC NULL.`**
Краш из SQL, доступный любому пользователю.

**`Fix VM clear WAL logging by registering VM blocks`** (Melanie Plageman).
Часть блока связанных коммитов: `Make VM clear take a RelFileLocator and not
fake relcache`, `Test that VM clear registers VM buffers`,
`Turn visibilitymap_clear() Assert back into an error`. Карта видимости
очищалась без корректной регистрации блоков в WAL — прямой путь к рассинхрону
после восстановления.

**`Include last block in FSM vacuum of bulk extended relation`** и
**`Update FSM after updating VM on-access`** — там же, карта свободного места.

**`Fix mishandling of leading '\' in nondeterministic LIKE`** и
**`Fix LIKE matching with nondeterministic collations and backslashes`**
(Peter Eisentraut). Недетерминированные сортировки продолжают преподносить
сюрпризы.

**`walsummarizer: Guard against WAL files whose tail ends are not valid`** и
**`Prevent walsummarizer from getting stuck at a timeline switch`**
(Robert Haas). Суммаризатор WAL зависал на переключении таймлайна.

**`Fix cascading standby reconnect failure after archive fallback.`**

**`Fix another empty nbtree index SSI race`** (Peter Geoghegan) — плюс четыре
коммита с новыми тестами: предикатные блокировки на пустых индексах, обратные
сканы, покрытие `_bt_set_startikey`.

**`unicode_case.c: defend against truncated UTF8`**,
**`pg_unicode_fast: fix final sigma logic`**,
**`pg_locale_libc.c: add missing casts to unsigned char`** (Jeff Davis). Греческая
финальная сигма — вечная тема.

**`On Windows, make link(2) report ENOTSUP when appropriate.`** Tom Lane, 31 июля,
последний коммит месяца из значимых.

---

# 💎 МЕЛОЧИ, КОТОРЫЕ ПОРАДУЮТ

**min() и max() для uuid** (`2e606d75c0b`, Masahiko Sawada, 1 июля):

```sql
SELECT min(id), max(id) FROM events;   -- теперь работает для uuid
```

У `uuid` давно был полный набор операторов сравнения и btree-класс — то есть тип
полностью упорядочен. Не хватало только агрегатов. Добавили `uuid_larger()` и
`uuid_smaller()`.

Красивая деталь из коммит-сообщения: значения UUID сравниваются лексикографически
по всем 128 битам. Для **UUIDv7**, где старшие биты — это Unix-timestamp, это
совпадает с хронологическим порядком. Значит `min()`/`max()` по UUIDv7 честно
возвращают самое старое и самое новое значение.

Там же по соседству: `Reject infinite and out-of-range interval shifts in
uuidv7()` — нельзя больше просить UUIDv7 из бесконечности.

---

**Имена индексов на выражениях** — два коммита Tom Lane
(`181b6185c79` + `Further improve...`):

```sql
CREATE INDEX ON t ((lower(name)));
-- было:  t_expr_idx
-- стало: осмысленное имя на основе выражения
```

Мелочь, но её ждали годами.

---

**Предупреждение про MD5** (`f6fdc2a4a73`, Fujii Masao):
аутентификация паролем с MD5-хешем теперь выдаёт warning. MD5 не удалили — но
намекнули очень прозрачно.

---

**`psql`: `pg_read_all_stats` теперь видит размер базы в `\l+`.** Раньше
столбец был пуст для не-суперпользователей.

---

**`psql`: `\df` больше не ломает автодополнение для процедур.**

---

**`pg_recvlogical` шлёт финальный feedback при SIGINT/SIGTERM.** Больше не
теряется прогресс при аккуратной остановке.

---

**`Disallow set-returning functions within window OVER clauses`** (Tom Lane) —
закрыт странный синтаксический уголок, который никто не должен был использовать.

---

**`Disallow renaming a rule to "_RETURN"`** — очевидное в ретроспективе.

---

**`Emit a warning when io_min_workers exceeds io_max_workers`** — AIO обрастает
защитой от дурака.

---

**`injection_points: Switch wait/wakeup to rely on atomics`** — инфраструктура
точек внедрения для тестов продолжает крепнуть. Плюс `Clear waiter slot on error
and exit` и `Make sure to detach injection points for re-attaching`.

---

**`Restore the ability to use | and -> as prefix operators`** (`0316593146d`,
Tom Lane, 18 июля, Bug #19558). Коммит `2f094e7ac` сделал `|` и `->`
встроенными именами операторов, корректно дав им приоритет `Op` и добавив
инфиксные продукции — но забыл про **префиксные**. Как минимум одно расширение
на это рассчитывало. Репортёр — Pierre Senellart.

---

# 👓 НЕВИДИМЫЙ ТРУД: кто читал патчи

У июля 28 коммиттеров. И **332 трейлера `Reviewed-by`** от **98 человек**.

```
  Fujii Masao          █████████████████████████████████████████████  21
  Tom Lane             ████████████████████████████████████  17
  Daniel Gustafsson    ██████████████████████████████████  16
  Michael Paquier      ██████████████████████████████  14
  Heikki Linnakangas   ███████████████████████████  13
  Amit Kapila          █████████████████████████  12
  Hayato Kuroda        █████████████████████  10
  Ewan Young           █████████████████████  10
  Chao Li              █████████████████████  10
  Ayush Tiwari         █████████████████████  10
  Andrey Borodin       ███████████████████  9
  Zsolt Parragi        █████████████████  8
  Tristan Partin       █████████████████  8
  Ashutosh Bapat       ███████████████  7
  Álvaro Herrera       █████████████  6
  Masahiko Sawada      █████████████  6
  ... и ещё 82 человека
```

Верхние строки — коммиттеры, читающие чужое сверх своего. Ниже начинается часть
проекта, которой нет в release notes: имя попадает в дерево только строкой
`Reviewed-by`.

Ревью в июле шло по тяжёлым местам, а не по опечаткам: карта видимости и её
WAL-логирование, карта свободного места, учёт времени ввода-вывода в
`pg_stat_io`, зависание `walsummarizer` на переключении таймлайна,
`vacuum_delay_point()` в GIN, `gistkillitems` для корневой страницы GiST,
атомики в injection points, покрытие обратных сканов nbtree. Под каждым таким
коммитом стоят чужие имена.

**Почти половина июльских ревьюеров — 47 из 98 — появляется ровно один раз.**
Вместе они дают 14% всех трейлеров: хвост широкий, но лёгкий. Верхняя десятка
закрывает 40%.

Полное распределение — сколько человек прочитали ровно N патчей:

```
  1 патч   ███████████████████████████████████████████████  47
  2        ███████████████  15
  3        ██████  6
  4        ██████████  10
  5        ████  4
  6        ██  2
  7        █  1
  8        ██  2
  9        █  1
  10       ████  4
  12–21    ██████  6   (Паке, Хейкки, Капила, Густафссон, Лейн, Фудзии)
```

Трейлер `Reviewed-by` стоит под **191 коммитом из 325** — то есть у 59%
попавшего в дерево в истории зафиксированы чужие глаза. У остальных сорока
процентов ревью могло быть и не записанным: тривиальные правки, откаты,
pgindent, синхронизация tzdata.

---

# 📅 ЧТО ДАЛЬШЕ

```
   PostgreSQL 20: карта цикла

   29 июня 2026   ┃  «Let the hacking begin ...»            ✔ было
   июль 2026      ┃  CF1  ── этот номер ──────────────      ✔ было
   сентябрь 2026  ┃  CF2                                    ← следующий
   ноябрь 2026    ┃  CF3
   январь 2027    ┃  CF4
   март 2027      ┃  CF5  (последний — feature freeze)
   ~ май 2027     ┃  beta 1
   ~ сент 2027    ┃  PostgreSQL 20 GA
```

**Что стоит на столе к сентябрю:**

- `GROUP BY ALL`, попытка №2 — с обещанным рефакторингом обработки `ORDER BY`
- `Reject concurrent sequence refreshes` — версия 3.0, после реверта и
  переделки
- `PL/Tcl 8.6` — вернётся ли требование после реверта
- `log_min_error_statement` и усечение — «can be considered separately»
- Как тестировать способность `pg_restore` читать архивы старше v10, когда
  такие серверы уже негде запустить

---

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  WAL STREET JOURNAL · выпуск №1                                              ║
║                                                                              ║
║  Составлено по `git log a281a3e6dbb..origin/master --until=2026-08-01`       ║
║  324 коммита · 28 коммиттеров · 182 имени в трейлерах · 736 файлов           ║
║                                                                              ║
║  Хеши коммитов сокращены до 11 символов. Полный текст любого:                ║
║      git show <hash>                                                         ║
║  Обсуждения: postgr.es/m/<message-id> из поля Discussion:                    ║
║                                                                              ║
║             «И вечный fsync, покой нам только снится»                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
```
