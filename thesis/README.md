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

Ветка отведена от `shared_archive_v7` (коммит `a2edc4d3531`), чтобы переключение
на неё не трогало рабочее дерево PostgreSQL и не инвалидировало сборку. Когда это
перестанет быть удобно:

```sh
git rebase --onto master a2edc4d3531 thesis
```

Каталог `thesis/` — единственное, что отличает эту ветку от базового коммита.
