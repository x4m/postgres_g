/*
 * gist_hilbert — альтернативный порядок, сохраняющий локальность, для
 * сортированного построения обобщённого дерева поиска.
 *
 * Смысл эксперимента (см. plan/60-pvldb.md, п. 3): показать, что смена кривой
 * обхода пространства требует одной функции класса операторов и не затрагивает
 * ни дерево, ни журналирование, ни конкурентность. Штатный порядок — Z-код
 * Мортона; здесь тем же интерфейсом подключается кривая Гильберта, у которой
 * локальность лучше (соседние по кривой точки всегда соседние в пространстве).
 *
 * Структура повторяет gist_point_sortsupport из ядра: ключ на листовой
 * странице — BOX, для точечного класса операторов вырожденный, сравниваем по
 * его нижнему углу.
 */
#include "postgres.h"

#include "access/gist.h"
#include "utils/geo_decls.h"
#include "utils/sortsupport.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gist_point_hilbert_sortsupport);

/* Порядок кривой: 2^ORDER точек по каждой оси. */
#define HILBERT_ORDER 31

/*
 * Преобразование float4 в целое с сохранением порядка — как в ядре:
 * для неотрицательных достаточно инвертировать знаковый бит, для
 * отрицательных — инвертировать все биты.
 */
static uint32
ieee_float32_to_uint32(float f)
{
	union
	{
		float		f;
		uint32		i;
	}			u;

	u.f = f;
	if ((u.i & 0x80000000) != 0)
		return ~u.i;
	else
		return u.i | 0x80000000;
}

/*
 * Индекс точки на кривой Гильберта порядка HILBERT_ORDER.
 * Классический алгоритм xy2d: обход разрядов от старшего к младшему с
 * поворотом квадранта.
 */
static uint64
hilbert_xy2d(uint32 x, uint32 y)
{
	uint64		d = 0;
	uint32		rx,
				ry;
	uint32		t;
	uint64		s;

	for (s = (uint64) 1 << (HILBERT_ORDER - 1); s > 0; s >>= 1)
	{
		rx = (x & (uint32) s) > 0 ? 1 : 0;
		ry = (y & (uint32) s) > 0 ? 1 : 0;
		d += s * s * ((3 * rx) ^ ry);

		/* поворот квадранта */
		if (ry == 0)
		{
			if (rx == 1)
			{
				x = (uint32) (s - 1) - x;
				y = (uint32) (s - 1) - y;
			}
			t = x;
			x = y;
			y = t;
		}
	}
	return d;
}

static uint64
point_hilbert_internal(float4 x, float4 y)
{
	/*
	 * Координаты приводятся к целым с сохранением порядка и усекаются до
	 * HILBERT_ORDER разрядов: кривая покрывает решётку 2^ORDER x 2^ORDER.
	 */
	uint32		ix = ieee_float32_to_uint32(x) >> (32 - HILBERT_ORDER);
	uint32		iy = ieee_float32_to_uint32(y) >> (32 - HILBERT_ORDER);

	return hilbert_xy2d(ix, iy);
}

static int
gist_bbox_hilbert_cmp(Datum a, Datum b, SortSupport ssup)
{
	BOX		   *b1 = DatumGetBoxP(a);
	BOX		   *b2 = DatumGetBoxP(b);
	uint64		h1,
				h2;

	h1 = point_hilbert_internal((float4) b1->low.x, (float4) b1->low.y);
	h2 = point_hilbert_internal((float4) b2->low.x, (float4) b2->low.y);

	if (h1 > h2)
		return 1;
	else if (h1 < h2)
		return -1;
	else
		return 0;
}

Datum
gist_point_hilbert_sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	/*
	 * Сокращённые ключи не используются: цель эксперимента — сравнить качество
	 * порядка, а не скорость сортировки, и сокращение внесло бы различие,
	 * не относящееся к предмету.
	 */
	ssup->comparator = gist_bbox_hilbert_cmp;
	PG_RETURN_VOID();
}
