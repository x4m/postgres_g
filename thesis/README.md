# thesis — докторская диссертация по GiST

Ветка форка PostgreSQL, в которой хранится **только текст**: план диссертации,
исходники диссертации и статей на LaTeX. Кода здесь нет — код живёт в upstream
PostgreSQL, ссылки на конкретные коммиты собраны в `plan/10-contributions.md`.

## Структура

```
thesis/
  plan/
    00-plan.md            план докторской: тема, специальность, главы, положения на защиту
    10-contributions.md   карта вкладов: тред в hackers → коммит → статья → глава
    20-publications.md    публикации: что есть, чего не хватает, план статей
    30-talks.md           доклады на PGCon, PGConf.dev, PGConf.Russia
    40-sources.md         разнарядка на ~200 источников, треды как первоисточники
    50-benchmarks.md      стенд, что на нём можно и нельзя мерить, очередь прогонов
    60-pvldb.md           план доработки P1 до PVLDB
  tex/
    common/preamble.tex   общая преамбула (XeLaTeX/tectonic, ГОСТ-подобное оформление)
    common/refs.bib       общая библиография
    diss/main.tex         диссертация, главы в diss/chapters/*.tex
    papers/               статьи, по каталогу на статью
  Makefile
```

## Сборка

Локально стоит `tectonic` (XeTeX), `pdflatex`/`latexmk` нет.

```sh
make -C thesis diss          # thesis/build/main.pdf
make -C thesis papers        # все статьи
make -C thesis clean
```

## Работа с веткой

Ветка отведена от тега `REL_18_0`: база фиксированная и не меняется, так что
переключение на ветку и обратно предсказуемо. Каталог `thesis/` — единственное,
что отличает её от релиза.

Переезд на другой релиз:

```sh
git rebase --onto REL_19_0 REL_18_0 thesis
```
