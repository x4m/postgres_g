#!/usr/bin/env python3
"""Накладывает прототип внутристраничного индексирования в памяти на дерево
исходников PostgreSQL. Скрипт, а не готовый diff: так правки остаются
читаемыми и переносятся между базовыми версиями.

Устройство — plan/90-intrapage-design.md. Кратко:
  * элементы внутренней страницы разбиваются на группы по sqrt(f);
  * для группы считается union её ключей и хранится как синтетический
    индексный кортеж;
  * при просмотре страницы consistent сначала вызывается для ключа группы,
    и при ложном ответе вся группа пропускается;
  * структура живёт в локальной для процесса хеш-таблице, актуальность
    проверяется по LSN страницы, строится не раньше порогового обращения.

Ограничения прототипа, сознательные:
  * только внутренние страницы. На листовых повторных обращений почти нет
    (измерено: доля повторов 0,06), а союз листовых ключей пришлось бы
    подавать в consistent как нелистовой ключ — лишний семантический риск;
  * только сканы без ORDER BY: для упорядочивающего обхода ключ группы дал бы
    бессмысленное расстояние;
  * вытеснение грубое: при переполнении кэш сбрасывается целиком.
"""
import re
import sys
import pathlib

SRC = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else '.')

# ---------------------------------------------------------------- gist_private.h
PRIVATE_H = SRC / 'src/include/access/gist_private.h'
h = PRIVATE_H.read_text()
if 'gist_page_skip' not in h:
    anchor = '/* gistget.c */'
    assert anchor in h, 'не найден якорь в gist_private.h'
    h = h.replace(anchor, '''/* gistget.c */
extern PGDLLIMPORT bool gist_page_skip;
extern PGDLLIMPORT int gist_page_skip_threshold;
extern void gist_page_skip_reset(void);
''' + anchor, 1)
    PRIVATE_H.write_text(h)
    print('патч: gist_private.h')

# ---------------------------------------------------------------- gistget.c
GISTGET = SRC / 'src/backend/access/gist/gistget.c'
g = GISTGET.read_text()

IMPL = r'''
/*
 * Внутристраничная структура пропуска.
 *
 * Просмотр узла стоит O(f) вызовов consistent, и по профилю на эту работу
 * уходит около половины процессорного времени спуска. Здесь элементы
 * внутренней страницы группируются, для группы считается union её ключей, и
 * consistent сначала вызывается для ключа группы: ложный ответ пропускает всю
 * группу.
 *
 * Корректность следует из инварианта GiST: ключ, покрывающий ключи группы,
 * ведёт себя как ключ внутреннего узла, а consistent для внутреннего ключа не
 * возвращает ложь, если хоть один элемент поддерева подходит. Поэтому пропуск
 * по ложному ответу не теряет результатов.
 *
 * Структура не хранится на странице: формат индексного кортежа, журнал и
 * восстановление не затрагиваются. Актуальность проверяется по LSN страницы.
 */

bool		gist_page_skip = false;
int			gist_page_skip_threshold = 3;

/* минимальное число элементов, при котором структура имеет смысл */
#define GIST_SKIP_MIN_ITEMS		32
/* потолок числа страниц в кэше; при переполнении кэш сбрасывается целиком */
#define GIST_SKIP_MAX_PAGES		8192

typedef struct GistSkipKey
{
	RelFileLocator locator;
	BlockNumber blkno;
} GistSkipKey;

typedef struct GistSkipGroup
{
	OffsetNumber first;
	OffsetNumber last;
	IndexTuple	key;			/* union ключей группы */
} GistSkipGroup;

typedef struct GistSkipEntry
{
	GistSkipKey key;			/* должен идти первым: ключ хеш-таблицы */
	XLogRecPtr	lsn;			/* LSN страницы на момент построения */
	int			visits;			/* сколько раз страница просматривалась */
	int			ngroups;
	GistSkipGroup *groups;
	MemoryContext cxt;			/* контекст групп, NULL если не построены */
} GistSkipEntry;

static HTAB *gist_skip_cache = NULL;
static MemoryContext gist_skip_cxt = NULL;

void
gist_page_skip_reset(void)
{
	if (gist_skip_cache)
	{
		hash_destroy(gist_skip_cache);
		gist_skip_cache = NULL;
	}
	if (gist_skip_cxt)
	{
		MemoryContextDelete(gist_skip_cxt);
		gist_skip_cxt = NULL;
	}
}

static void
gist_skip_init(void)
{
	HASHCTL		ctl;

	gist_skip_cxt = AllocSetContextCreate(TopMemoryContext,
										  "GiST page skip cache",
										  ALLOCSET_DEFAULT_SIZES);
	ctl.keysize = sizeof(GistSkipKey);
	ctl.entrysize = sizeof(GistSkipEntry);
	ctl.hcxt = gist_skip_cxt;
	gist_skip_cache = hash_create("GiST page skip", 256, &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Построить группы для внутренней страницы. Размер группы берётся около
 * sqrt(f): он минимизирует f/g + g, то есть сумму проверок групп и проверок
 * элементов совпавшей группы.
 */
static void
gist_skip_build(GistSkipEntry *entry, Relation r, Page page,
				GISTSTATE *giststate)
{
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	int			gsize;
	int			ngroups;
	MemoryContext oldcxt;
	IndexTuple *itvec;
	int			i;

	if (maxoff < GIST_SKIP_MIN_ITEMS)
		return;

	gsize = (int) ceil(sqrt((double) maxoff));
	if (gsize < 2)
		return;
	ngroups = (maxoff + gsize - 1) / gsize;

	entry->cxt = AllocSetContextCreate(gist_skip_cxt,
									   "GiST page skip entry",
									   ALLOCSET_SMALL_SIZES);
	oldcxt = MemoryContextSwitchTo(entry->cxt);
	entry->groups = palloc0(sizeof(GistSkipGroup) * ngroups);
	itvec = palloc(sizeof(IndexTuple) * gsize);

	for (i = 0; i < ngroups; i++)
	{
		OffsetNumber first = FirstOffsetNumber + i * gsize;
		OffsetNumber last = first + gsize - 1;
		int			len = 0;
		OffsetNumber o;
		Datum		attr[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];

		if (last > maxoff)
			last = maxoff;

		for (o = first; o <= last; o++)
		{
			ItemId		iid = PageGetItemId(page, o);

			if (!ItemIdIsUsed(iid))
				continue;
			itvec[len++] = (IndexTuple) PageGetItem(page, iid);
		}

		entry->groups[i].first = first;
		entry->groups[i].last = last;

		if (len == 0)
		{
			entry->groups[i].key = NULL;
			continue;
		}

		/*
		 * Недействительные кортежи, оставшиеся от версий до 9.1, означают
		 * «спускаться всегда». Группу с таким кортежем не сворачиваем.
		 */
		{
			bool		invalid = false;
			int			k;

			for (k = 0; k < len; k++)
				if (GistTupleIsInvalid(itvec[k]))
					invalid = true;
			if (invalid)
			{
				entry->groups[i].key = NULL;
				continue;
			}
		}

		gistMakeUnionItVec(giststate, itvec, len, attr, isnull);
		entry->groups[i].key = gistFormTuple(giststate, r, attr, isnull, false);
	}

	entry->ngroups = ngroups;
	MemoryContextSwitchTo(oldcxt);
}

/*
 * Вернуть структуру пропуска для страницы, построив её при необходимости.
 * NULL означает «просматривать страницу как обычно».
 */
static GistSkipEntry *
gist_skip_lookup(Relation r, Buffer buffer, Page page, GISTSTATE *giststate)
{
	GistSkipKey key;
	GistSkipEntry *entry;
	bool		found;
	XLogRecPtr	lsn;

	if (!gist_page_skip || GistPageIsLeaf(page))
		return NULL;

	if (gist_skip_cache == NULL)
		gist_skip_init();
	else if (hash_get_num_entries(gist_skip_cache) > GIST_SKIP_MAX_PAGES)
	{
		/* грубое вытеснение: для прототипа достаточно */
		gist_page_skip_reset();
		gist_skip_init();
	}

	memset(&key, 0, sizeof(key));
	key.locator = r->rd_locator;
	key.blkno = BufferGetBlockNumber(buffer);

	entry = (GistSkipEntry *) hash_search(gist_skip_cache, &key,
										  HASH_ENTER, &found);
	lsn = BufferGetLSNAtomic(buffer);

	if (!found)
	{
		entry->lsn = lsn;
		entry->visits = 0;
		entry->ngroups = 0;
		entry->groups = NULL;
		entry->cxt = NULL;
	}
	else if (entry->lsn != lsn)
	{
		/* страница изменилась: построенное больше не годится */
		if (entry->cxt)
			MemoryContextDelete(entry->cxt);
		entry->lsn = lsn;
		entry->visits = 0;
		entry->ngroups = 0;
		entry->groups = NULL;
		entry->cxt = NULL;
	}

	entry->visits++;

	if (entry->cxt == NULL && entry->visits >= gist_page_skip_threshold)
		gist_skip_build(entry, r, page, giststate);

	return (entry->ngroups > 0) ? entry : NULL;
}
'''

if 'gist_skip_lookup' not in g:
    anchor = '/*\n * Scan all items on the GiST index page identified by *pageItem, and insert'
    assert anchor in g, 'не найден якорь перед gistScanPage'
    g = g.replace(anchor, IMPL + '\n' + anchor, 1)

    # заголовки
    g = g.replace('#include "access/relscan.h"',
                  '#include "access/relscan.h"\n#include "utils/hsearch.h"\n#include "utils/memutils.h"',
                  1)

    # интеграция в цикл просмотра
    old_loop = '''	maxoff = PageGetMaxOffsetNumber(page);
	for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
	{
		ItemId		iid = PageGetItemId(page, i);'''
    new_loop = '''	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * Структура пропуска строится только для внутренних страниц и только для
	 * сканов без упорядочивания: ключ группы дал бы бессмысленное расстояние.
	 */
	skip = (scan->numberOfOrderBys == 0)
		? gist_skip_lookup(r, buffer, page, giststate) : NULL;

	for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
	{
		ItemId		iid = PageGetItemId(page, i);

		/* если началась группа, проверить её ключ и, возможно, пропустить */
		while (skip && skip_grp < skip->ngroups &&
			   i > skip->groups[skip_grp].last)
			skip_grp++;
		if (skip && skip_grp < skip->ngroups &&
			i == skip->groups[skip_grp].first &&
			skip->groups[skip_grp].key != NULL)
		{
			bool		gmatch;
			bool		grecheck;
			bool		grecheck_d;

			oldcxt = MemoryContextSwitchTo(so->giststate->tempCxt);
			gmatch = gistindex_keytest(scan, skip->groups[skip_grp].key,
									   page, i, &grecheck, &grecheck_d);
			MemoryContextSwitchTo(oldcxt);
			MemoryContextReset(so->giststate->tempCxt);

			if (!gmatch)
			{
				i = skip->groups[skip_grp].last;
				skip_grp++;
				continue;
			}
		}'''
    assert old_loop in g, 'не найден цикл просмотра'
    g = g.replace(old_loop, new_loop, 1)

    # объявления в gistScanPage
    old_decl = '''	OffsetNumber maxoff;
	OffsetNumber i;
	MemoryContext oldcxt;

	Assert(!GISTSearchItemIsHeap(*pageItem));'''
    new_decl = '''	OffsetNumber maxoff;
	OffsetNumber i;
	MemoryContext oldcxt;
	GistSkipEntry *skip;
	int			skip_grp = 0;

	Assert(!GISTSearchItemIsHeap(*pageItem));'''
    assert old_decl in g, 'не найдены объявления в gistScanPage'
    g = g.replace(old_decl, new_decl, 1)

    GISTGET.write_text(g)
    print('патч: gistget.c')

# ---------------------------------------------------------------- guc_tables.c
GUC = SRC / 'src/backend/utils/misc/guc_tables.c'
u = GUC.read_text()
if 'gist_page_skip' not in u:
    anchor = '\t/* End-of-list marker */\n\t{\n\t\t{NULL, 0, 0, NULL, NULL}, NULL, false, NULL, NULL, NULL\n\t}\n};'
    assert anchor in u, 'не найден конец списка булевых параметров'
    u = u.replace(anchor, '''\t{
\t\t{"gist_page_skip", PGC_USERSET, QUERY_TUNING_METHOD,
\t\t\tgettext_noop("Enables intra-page skipping in GiST scans."),
\t\t\tgettext_noop("Groups entries of internal pages and tests the group key "
\t\t\t\t\t\t "before testing entries.")
\t\t},
\t\t&gist_page_skip,
\t\tfalse,
\t\tNULL, NULL, NULL
\t},

''' + anchor, 1)
    # порог
    anchor_int = '\t/* End-of-list marker */\n\t{\n\t\t{NULL, 0, 0, NULL, NULL}, NULL, 0, 0, 0, NULL, NULL, NULL\n\t}\n};'
    assert anchor_int in u, 'не найден конец списка целочисленных параметров'
    u = u.replace(anchor_int, '''\t{
\t\t{"gist_page_skip_threshold", PGC_USERSET, QUERY_TUNING_METHOD,
\t\t\tgettext_noop("Number of visits before a GiST page skip structure is built."),
\t\t\tNULL
\t\t},
\t\t&gist_page_skip_threshold,
\t\t3, 1, 1000,
\t\tNULL, NULL, NULL
\t},

''' + anchor_int, 1)
    u = u.replace('#include "access/gin.h"', '#include "access/gin.h"\n#include "access/gist_private.h"', 1)
    GUC.write_text(u)
    print('патч: guc_tables.c')

print('готово')
