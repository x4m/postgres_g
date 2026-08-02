# Раскрытие использования ИИ

Требования издателей различаются по формулировке и по месту размещения.

- **Elsevier** (Information Systems): отдельное заявление «Declaration of
  generative AI and AI-assisted technologies in the writing process» в конце
  рукописи, перед списком литературы. В разделе о методах не размещается.
- **Wiley** (SP&E и другие): раскрытие в разделе Methods, с описанием того, что
  именно было создано, и с названием модели или инструмента.

Процитированный в форме текст — формулировка Wiley. Если форма относится к
Information Systems, стоит перепроверить, чью политику она показывает.

Общее у обоих издателей: инструмент не может быть автором; ответственность за
содержание несёт автор; описание должно быть конкретным.

---

## Вариант для Elsevier

> **Declaration of generative AI and AI-assisted technologies in the writing
> process.** During the preparation of this work the author used Claude
> (Anthropic) for the following: drafting the text of the manuscript;
> implementing the measurement scripts and the experimental Hilbert-order
> comparator described in Section~8; running the experiments and tabulating
> their results; producing the figures; and searching for and assembling the
> bibliography. The algorithms described in this paper, their implementation in
> PostgreSQL, and the deployment experience reported in Section~9 predate and
> are independent of that use. All measurements were re-checked by the author
> against the recorded scripts and outputs, which are available with the
> artifact. After using this tool the author reviewed and edited the content as
> needed and takes full responsibility for the content of the publication.

## Вариант для Wiley (в раздел о методике)

> The manuscript was prepared with the assistance of Claude (Anthropic), a
> large language model. The tool was used to draft the text of all sections, to
> implement the measurement scripts and the experimental Hilbert-order
> comparator of Section~8, to run the reported experiments and tabulate their
> results, to produce the figures, and to search for and assemble the
> bibliography. It was not used to design the algorithms, which were
> implemented in PostgreSQL between 2020 and 2025 and are described here after
> the fact, nor to interpret the deployment history of Section~9. The author
> verified every reported measurement against the recorded scripts and outputs,
> reviewed and edited all generated text, and takes full responsibility for the
> content. The tool is not, and is not listed as, an author.

---

## Что здесь важно решить, а не просто заполнить

Оба издателя пишут, что созданное ИИ содержание **не поощряется**. Заявление
такого объёма, как выше, — честное, но широкое, и редактор вправе отнестись к
нему настороженно.

Есть три пути.

**1. Раскрыть как есть.** Честно, соответствует требованиям, но повышает риск
на стадии редакторского решения. Заявление при этом стоит сопроводить тем, что
редакторы обычно и хотят услышать: результаты работы существуют независимо от
текста, код в выпусках 14, 15 и 18, обсуждения в открытом архиве, измерения
воспроизводимы по скриптам.

**2. Переписать текст самому.** Тогда раскрытие сужается до скриптов измерений
и подбора литературы, а это уже привычная для редакций категория. Работа
немаленькая: 22 страницы. Но содержание, структура и все утверждения уже
зафиксированы, и переписывание идёт быстрее, чем письмо с нуля.

**3. Промежуточный путь.** Переписать своими словами то, что несёт авторскую
позицию, — введение, раздел об опыте эксплуатации, заключение, — и оставить как
есть описательные части: обзор, модель стоимости, таблицы. Раскрытие тогда
конкретно и проверяемо: сказано, какие разделы затронуты.

Решение за автором. Что бы ни было выбрано, заявление должно совпадать с тем,
что произошло на самом деле: расхождение здесь хуже любого из вариантов.

## Чего в заявлении быть не должно

- Формулировки «использовался для редактирования и проверки языка» — это не то,
  что происходило.
- Умолчания о том, что скрипты измерений и расширение с кривой Гильберта
  написаны инструментом. Они лежат в `bench/scripts/` и в артефакте, и это
  проверяемо.
- Указания инструмента среди авторов или в благодарностях в роли участника
  исследования.
