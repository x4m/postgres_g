/*
 *	qsort_interruptible.c: qsort_arg that includes CHECK_FOR_INTERRUPTS
 */

#include "postgres.h"
#include "miscadmin.h"

#define ST_SORT qsort_interruptible
#define ST_ELEMENT_TYPE_VOID
#define ST_COMPARATOR_TYPE_NAME qsort_arg_comparator
#define ST_COMPARE_RUNTIME_POINTER
#define ST_COMPARE_ARG_TYPE void
#define ST_SCOPE
#define ST_DEFINE
#define ST_CHECK_FOR_INTERRUPTS
#include "lib/sort_template.h"

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

#define ST_SORT sort_int32_impl
#define ST_ELEMENT_TYPE int32
#define ST_COMPARE(a, b, ascending) sort_int32_cmp(a, b, ascending)
#define ST_COMPARE_ARG_TYPE bool
#define ST_SCOPE static
#define ST_DECLARE
#define ST_DEFINE
#include "lib/sort_template.h"

void sort_int32(int32 *base, size_t nel, bool ascending)
{
	sort_int32_impl(base, nel, &ascending);
}
