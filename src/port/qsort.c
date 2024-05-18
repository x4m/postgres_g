/*
 *	qsort.c: standard quicksort algorithm
 */

#include "c.h"

#define ST_SORT pg_qsort
#define ST_ELEMENT_TYPE_VOID
#define ST_COMPARE_RUNTIME_POINTER
#define ST_SCOPE
#define ST_DECLARE
#define ST_DEFINE
#include "lib/sort_template.h"

/*
 * qsort comparator wrapper for strcmp.
 */
int
pg_qsort_strcmp(const void *a, const void *b)
{
	return strcmp(*(const char *const *) a, *(const char *const *) b);
}

static inline int
sort_int32_cmp(int32* a, int32* b, bool* ascending)
{
	if (*ascending)
	{
		if (*a < *b)
			return -1;
		if (*a > *b)
			return 1;
	}
	else
	{
		if (*a < *b)
			return 1;
		if (*a > *b)
			return -1;
	}
	return 0;
}

static inline int
sort_int32_cmp_2(int* a, int* b, bool* ascending)
{
	int result;

	if (*a < *b)
		result = -1;
	else if (*a > *b)
		result = 1;
	else
		result = 0;

	if (!*ascending)
		result = -result;

	return result;
}

static inline int
sort_int32_cmp_3(int* a, int* b, bool* ascending)
{
	int result;
	bool asc = *ascending;

	if (*a < *b)
		result = -1;
	else if (*a > *b)
		result = 1;
	else
		result = 0;

	if (!asc)
		result = -result;

	return result;
}

#define ST_SORT sort_int32_impl
#define ST_ELEMENT_TYPE int32
#define ST_COMPARE(a, b, ascending) sort_int32_cmp(a, b, ascending)
//#define ST_COMPARE(a, b, ascending) sort_int32_cmp_2(a, b, ascending)
//#define ST_COMPARE(a, b, ascending) sort_int32_cmp_3(a, b, ascending)
#define ST_COMPARE_ARG_TYPE bool
#define ST_SCOPE
#define ST_DECLARE
#define ST_DEFINE
#include "lib/sort_template.h"

void sort_int32(int32 *base, size_t nel, bool ascending)
{
	sort_int32_impl(base, nel, &ascending);
}
