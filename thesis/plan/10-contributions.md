# Карта вкладов: тред → коммит → статья → глава

Инвентаризация работ по индексным методам доступа в PostgreSQL. Треды — из
локального архива pgsql-hackers (`hackorum`, БД на порту 1337, `topics.id`),
коммиты — из `~/postgres`.

Запрос, которым получен список тредов:

```sql
SELECT t.id, t.created_at::date, t.message_count, t.title
  FROM topics t
 WHERE t.creator_person_id = 12432   -- Andrey Borodin
   AND t.title ~* 'gist|gin|index|sort|cube|amcheck|btree'
 ORDER BY t.created_at;
```

## 1. Ветвистость дерева и раскладка страницы → глава 3

| Тред | Коммит | Статус | Публикация |
|------|--------|--------|-----------|
| 34137 (2016-02) Improvement of GiST page layout | — | идея, не реализована | — |
| 34876 (2016-07, 36 писем) GiST optimizing memmoves in gistplacetopage | `b1328d78f88` Invent PageIndexTupleOverwrite (2016-09) | в upstream, PG 10 | BDAS 2017 (51 цит.) |
| 36781 (2017-05, 23 письма) GiST opclasses without compress/decompress | `d3a4f89d8a3` Allow no-op GiST support functions to be omitted (2017-09) | в upstream, PG 11 | **нет — P4** |
| 38541 (2018-04, 28 писем) Covering GiST indexes | `f2e403803fe` Support for INCLUDE attributes in GiST (2019-03) | в upstream, PG 12 | **нет — P4** |
| — | `09c1c6ab4bc` INCLUDE'd columns in SP-GiST (2021-04) | в upstream, PG 14 | **нет — P4** |
| 38169 (2018-02) [WiP] GiST intrapage indexing | — | **не доведено** | **нет — P4** |

Внутристраничное индексирование — это ровно тот пункт, который в план-проспекте
2016 г. был заявлен как «разрабатывается алгоритм внутристраничного
индексирования». Единственная позиция плана, где нужен новый код.

## 2. Функция штрафа и качество разбиения → глава 3

| Тред | Коммит | Статус | Публикация |
|------|--------|--------|-----------|
| 35173 (2016-08, 28 писем) GiST penalty functions [PoC] | — | в upstream не принято | IEEE ICBDA 2017 (24 цит.) |
| 35276 (2016-09) GiST: interpretation of NaN from penalty | `e5d8f359610` Fix Inf/NaN in GiST pairing heap comparator (2019-09) | в upstream | — |

## 3. Построение индекса сортировкой → глава 4, статья P1

| Тред | Коммит | Статус |
|------|--------|--------|
| 41130 (2019-08, **138 писем**) Yet another fast GiST build | `16fa9b2b30a` Add support for building GiST index by sorting (2020-09) | в upstream, PG 14 |
| — | `265ea567852` Set right-links during sorted GiST index build (2020-10) | в upstream |
| — | `6f0bc5e1daf` Fix missing validation for new GiST sortsupport (2020-10) | в upstream |
| — | `9f984ba6d23` sortsupport for gist_btree opclasses (2021-04) → откат `d92b1cdbab4` в тот же день | откачено |
| — | `f1ea98a7975` Reduce non-leaf keys overlap in GiST produced by sorted build (2022-02) | в upstream, PG 15 |
| — | `e4309f73f69` Add support for sorted gist index builds to btree_gist (2025-04) | в upstream, PG 18 — возврат отката 2021 г. |
| 42960 (2020-09) Batching page logging during B-tree build | — | смежный результат |
| 49652 (2024-05, 38 писем) Sort functions with specialized comparators | `53d3daa491b` Specialize intarray sorting (2025-02), `30229be755e` macaddr SortSupport (2026-04) | в upstream |

Ключевой результат: сборка GiST через Z-order даёт кратное ускорение построения
индекса, а `f1ea98a7975` снимает главный побочный эффект — рост перекрытия
нелистовых ключей, из-за которого выигрыш на построении съедался проигрышем на
поиске. Именно эта пара результатов делает статью P1 самостоятельной.

## 4. Сборка мусора и конкурентность → глава 5, статья P2

| Тред | Коммит | Статус |
|------|--------|--------|
| 38336 (2018-03, **82 письма**) GiST VACUUM | `fe280694d0d` Scan GiST indexes in physical order during VACUUM (2019-03) | в upstream, PG 12 |
| — | `7df159a620b` Delete empty pages during GiST VACUUM (2019-03) | в upstream, PG 12 |
| — | `9eb5607e699` Refactor checks for deleted GiST pages (2019-07) | в upstream |
| — | `6655a7299d8` Use full 64-bit XID for deleted GiST page age (2019-07) | в upstream, PG 13 |
| 38996 (2018-07) Legacy GiST invalid tuples | — | смежное |
| 35648 (2016-11) GIN non-intrusive vacuum of posting tree | `218f51584d5` Reduce page locking in GIN vacuum (2017-03) | **частично отменено** — см. ниже |
| 253137 (2026-07) Delete GIN posting tree pages without excessive locking | — | открыт |
| 50415 (2024-10, 36 писем) Using read_stream in index vacuum | `c5c239e26e3` btree, `69273b818b1` GiST, `e215166c9c8` SP-GiST (2025-03) | в upstream, PG 19 |
| 52165 (2025-08) [WiP] B-tree page merge during vacuum | — | открыт |

**Откат `218f51584d5`.** Коммит состоял из двух частей: (1) захватывать
блокировку очистки только когда есть что удалять, (2) блокировать поддерево, а
не всё дерево вхождений. Часть 2 отменена коммитом `fd83c83d094` «Fix deadlock
in GIN vacuum introduced by 218f51584d5» (2018-12-13, бэкпорт во все
поддерживаемые выпуски): при конкурентном расщеплении родителя вставка не
удерживает закрепления всех страниц пути от корня к листу, откуда цикл ожидания
со сборкой мусора; неинвазивного решения не нашли. Часть 1 сохранена и даёт
основной практический эффект. Сопутствующие: `52ac6cd2d0c` (страницы могли
переиспользоваться до обращения к ним сканированием, которое спускается не от
корня; момент удаления записывается в `pd_prune_xid` из-за двоичной
совместимости, бэкпорт до 9.4), `c6ade7a8cd3`, `e14641197a5`.

Значение для диссертации: статья JPCS 2018 «Improving generalized inverted index
lock wait times» описывала в том числе отменённую часть. Статья прошла
рецензирование, код не прошёл эксплуатацию. Разбирать этот случай явно — §5.4
диссертации и «Методология» введения; ссылаться на JPCS 2018 как на публикацию,
раскрывающую положение П4, нельзя.

GiST VACUUM (82 письма, четыре коммита, переход на 64-битные XID) не
опубликован вовсе — это P2.

## 5. Предикатные блокировки и изоляция → глава 5, статья P5

| Тред | Коммит |
|------|--------|
| — | `3ad55863e93` Add predicate locking for GiST (2018-03), PG 11 |
| — | `0bef1c0678d` Re-think predicate locking on GIN indexes (2018-05), PG 11 |
| 70463 (2020-12, **147 писем**) CREATE INDEX CONCURRENTLY does not index prepared xact's data | `3cd9c3b9219`, `fdd965d074d`, `8a54e12a38d` (2021) | в upstream, бэкпорт |

## 6. Верификация корректности индексов → глава 6, статья P3

| Тред | Коммит |
|------|--------|
| 39396 (2018-09, 31 письмо) amcheck verification for GiST | — (первый заход) |
| 41208 (2019-09) Amcheck: rightlink verification with lock coupling | `39132b784ae` Teach amcheck to verify sibling links in all cases (2019-08) |
| 41060 (2019-08) Do not check unlogged indexes on standby | `6754fe65a4c` amcheck: skip unlogged relations during recovery (2019-08) |
| 46031 (2022-05, **92 письма**) Amcheck verification of GiST and GIN | `d70b17636dd` common routines, `14ffaece0fb` gin_index_check (2025-03) + серия исправлений `0cf205e122a`, `cdd1a431f21`, `0b54b392334` (2025-06) | в upstream, PG 18 |
| 51827 (2025-06) amcheck: support for GiST | — | открыт |
| 53329 (2026-02) amcheck: index-all-keys-match verification for B-Tree | — | открыт |
| — | `b1fe8efdf17`, `ab65dfb0fb2` нормализация индексных кортежей (2024-03) | в upstream |
| 69237 (2019-06) Logging corruption error codes | `8ec97e78a77` error codes on VM corruption (2025-09) | в upstream |

Прямое продолжение BDAS 2016 «Database Index Debug Techniques»: там был описан
подход к отладке индексов, здесь он доведён до штатного средства верификации
инвариантов в составе СУБД.

## 7. Многомерные типы и доступ к ним → глава 2

| Тред | Коммит |
|------|--------|
| 36765 / 37623 (2017) Index only scan for cube and seg | `de1d042f597` Support index-only scans in cube and seg (2017-11), PG 11 |
| — | `f919c165ebd` Enforce cube dimension limit (2018-08) |
| — | `563a053bdd4`, `f50c80dbb17` поведение оператора `~>` (cube, int) (2018-01) |
| — | `2a6368343ff` KNN-поиск в SP-GiST (2018-09), PG 12 |
| 36473 (2017-04) Merge join for GiST | — | не принято |
| — | `756ab29124d`, `fa41cf8f183`, `9e596b65f43` pageinspect для GiST (2021) |

## Смежное, вне темы диссертации

UUID v7, SLRU/MultiXact, pglz и сжатие FPI, transaction timeout, archive_mode,
WAL-совместимость, VM corruption, GSoC/commitfest-менеджмент. В список трудов
войдёт, в положения на защиту — нет.

## Сводка

- Тредов по индексным методам, начатых лично: **27**.
- Из них доведено до коммита в upstream: около 20 результатов.
- Опубликовано: 4 работы (BDAS 2016/2017, ICBDA 2017, JPCS 2018).
- **Не опубликовано: сортированная сборка, GiST VACUUM, amcheck, INCLUDE и
  ветвистость, предикатные блокировки, read_stream** — шесть законченных
  результатов, каждый тянет на статью.
