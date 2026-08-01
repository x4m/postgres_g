# Список литературы: как набрать ~200 источников

По традиции список литературы докторской по техническим наукам содержит порядка
200 наименований. Сейчас в `tex/common/refs.bib` — 18. Ниже разнарядка по
разделам: откуда брать и сколько.

## Разнарядка

| Раздел | Ожидаемо | Источники |
|--------|----------|-----------|
| Пространственное индексирование: R-дерево и производные | 25–30 | Guttman, Beckmann (R*, RR*), Sellis (R+), Kamel–Faloutsos (Hilbert R-tree), Berchtold (X-tree), White–Jain (SS-tree), Katayama (SR-tree), Leutenegger (STR packing), Roussopoulos (nearest neighbor) |
| Обобщённые и расширяемые методы доступа | 15–20 | Hellerstein (GiST), Kornacker (конкурентность, amdb), Aoki (расширяемость), SP-GiST (Aref–Ilyas), обзоры Gaede–Günther, Samet |
| B-дерево, конкурентность, восстановление | 15–20 | Bayer–McCreight, Lehman–Yao, Mohan (ARIES, ARIES/IM, ARIES/KVL), Graefe (обзор B-tree, современные техники), Sagiv |
| Построение индексов, внешняя сортировка | 10–15 | Aggarwal–Vitter (модель ввода-вывода), Vitter (external memory), bulk loading R-деревьев, Graefe (implementing sorting) |
| Кривые, сохраняющие локальность | 10–12 | Morton, Orenstein (Z-order), Faloutsos (Hilbert), Moon (анализ Hilbert), Lawder |
| Изоляция, предикатные блокировки, MVCC | 10–12 | Gray, Eswaran (предикатные блокировки), Berenson (уровни изоляции), Cahill–Röhm–Fekete (SSI), Ports–Grittner |
| Сборка мусора и обслуживание индексов | 8–10 | работы по vacuum, page reclamation, safe memory reclamation (epoch/QSBR), Bohannon |
| Верификация и целостность данных | 10–12 | работы по проверке структур данных, invariant checking, silent data corruption, Bairavasundaram, Prabhakaran (IRON) |
| Высокая размерность и её следствия | 10–12 | Beyer (when is NN meaningful), Weber (VA-file), Böhm (обзор high-dimensional indexing), Indyk–Motwani (LSH) |
| Аналитические данные, OLAP, агрегирование | 12–15 | Gray (data cube), Chaudhuri–Dayal, Kimball, работы по iceberg-запросам; собственные работы 2008–2013 |
| Векторный поиск и современные применения GiST | 8–10 | HNSW (Malkov), IVF, pgvector, обзоры ANN |
| Реализация PostgreSQL, документация, книги | 10–15 | Rogov (PostgreSQL 18 изнутри), Angelakos, документация PostgreSQL по главам, README подсистем |
| Собственные публикации | 15–20 | см. `20-publications.md` |
| Стандарты, ГОСТы, нормативные документы | 3–5 | ГОСТ Р 7.0.11-2011 и др. |

Итого ~200.

## Где брать

1. **Списки литературы своих же статей** — BDAS 2016/2017, ICBDA 2017, JPCS 2018
   (`Study/Статьи/*/`, `ref_1701.bib` из bdas17 уже частично перенесён).
2. **Кандидатская** — 105 наименований, `Study/Главы/текст.docx`. Значительная
   часть по R-деревьям и OLAP переносится напрямую.
3. **Google Scholar** — цитирующие работы: кто ссылается на BDAS 2017 и ICBDA
   2017, тот работает по смежной теме, и эти работы обязаны быть в обзоре.
4. **Ссылки в коде PostgreSQL** — `src/backend/access/*/README` содержат ссылки
   на первоисточники алгоритмов; это самый точный способ увязать реализацию с
   литературой.
5. **Обсуждения в pgsql-hackers** (`hackorum`, порт 1337) — в тредах регулярно
   приводятся ссылки на статьи; запрос по телу писем даёт готовые кандидатуры:
   ```sql
   SELECT DISTINCT substring(body from '10\.[0-9]{4}/[^ )>,]+') FROM messages
    WHERE topic_id IN (34876,35173,38336,41130,46031) AND body ~ '10\.[0-9]{4}/';
   ```

## Дисциплина

- Каждый источник должен быть реально прочитан и процитирован в тексте;
  «мёртвые» ссылки диссовет замечает.
- Соотношение: не менее трети источников — за последние 5 лет, иначе обзор
  выглядит устаревшим.
- Самоцитирование — 15–20 наименований, это норма и требование (положения
  должны быть опубликованы).
- Оформление по ГОСТ 7.0.5-2008; стиль `ugost2008.bst` уже подключён.
