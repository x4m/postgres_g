/*
 * contrib/btree_gist/btree_utils_num.h
 */
#ifndef __BTREE_UTILS_NUM_H__
#define __BTREE_UTILS_NUM_H__

#include <math.h>
#include <float.h>

#include "access/gist.h"
#include "btree_gist.h"

typedef char GBT_NUMKEY;

/* Better readable key */
typedef struct
{
	const GBT_NUMKEY *lower,
			   *upper;
} GBT_NUMKEY_R;


/* for sorting */
typedef struct
{
	int			i;
	GBT_NUMKEY *t;
} Nsrt;


/* type description */

/*
 * Cross-type comparison support.
 *
 * For cross-type operator support an opclass may supply a NULL-terminated
 * array of gbt_subtype_info entries, one per supported query subtype. Each
 * callback receives the index key in its native C representation (pointer
 * into the compressed key) and a context describing the query value.
 */
typedef struct gbt_subtype_context
{
	Datum		query;			/* Datum of cxt->subtype */
	Oid			subtype;		/* right-hand/query operand type */
	Oid			collation;		/* collation from the support call */
	FmgrInfo   *flinfo;			/* support-function call context */
	void	   *query_cache;	/* callback-owned per-call state */
} gbt_subtype_context;

typedef struct gbt_subtype_info
{
	Oid			subtype;		/* InvalidOid terminates the array */
	bool		(*f_lt) (const void *key, gbt_subtype_context *cxt);
	bool		(*f_le) (const void *key, gbt_subtype_context *cxt);
	bool		(*f_eq) (const void *key, gbt_subtype_context *cxt);
	bool		(*f_ge) (const void *key, gbt_subtype_context *cxt);
	bool		(*f_gt) (const void *key, gbt_subtype_context *cxt);

	/* NULL if no KNN */
	float8		(*f_dist) (const void *key, gbt_subtype_context *cxt);
} gbt_subtype_info;

typedef struct
{

	/* Attribs */

	enum gbtree_type t;			/* data type */
	int32		size;			/* size of type, 0 means variable */
	int32		indexsize;		/* size of datums stored in index */

	/* Methods */

	bool		(*f_gt) (const void *, const void *, FmgrInfo *);	/* greater than */
	bool		(*f_ge) (const void *, const void *, FmgrInfo *);	/* greater or equal */
	bool		(*f_eq) (const void *, const void *, FmgrInfo *);	/* equal */
	bool		(*f_le) (const void *, const void *, FmgrInfo *);	/* less or equal */
	bool		(*f_lt) (const void *, const void *, FmgrInfo *);	/* less than */
	int			(*f_cmp) (const void *, const void *, FmgrInfo *);	/* key compare function */
	float8		(*f_dist) (const void *, const void *, FmgrInfo *); /* key distance function */

	/*
	 * Optional NULL-terminated array of cross-type comparison callbacks. NULL
	 * if the opclass only supports same-type comparisons.
	 */
	const gbt_subtype_info *subtype_ops;

	/*
	 * Native pg_type OID of the indexed type. Used by the _x APIs to validate
	 * the subtype passed in from the planner. InvalidOid disables that check,
	 * which is right for legacy opclasses that don't use _x.
	 */
	Oid			type_oid;
} gbtree_ninfo;

#define GBT_DEFINE_INT_CROSSTYPE(prefix, key_ctype, get_sub) \
static bool \
prefix##_lt(const void *k, gbt_subtype_context *cxt) \
{ \
	return (int64) *(const key_ctype *) k < (int64) get_sub(cxt->query); \
} \
static bool \
prefix##_le(const void *k, gbt_subtype_context *cxt) \
{ \
	return (int64) *(const key_ctype *) k <= (int64) get_sub(cxt->query); \
} \
static bool \
prefix##_eq(const void *k, gbt_subtype_context *cxt) \
{ \
	return (int64) *(const key_ctype *) k == (int64) get_sub(cxt->query); \
} \
static bool \
prefix##_ge(const void *k, gbt_subtype_context *cxt) \
{ \
	return (int64) *(const key_ctype *) k >= (int64) get_sub(cxt->query); \
} \
static bool \
prefix##_gt(const void *k, gbt_subtype_context *cxt) \
{ \
	return (int64) *(const key_ctype *) k > (int64) get_sub(cxt->query); \
} \
static float8 \
prefix##_dist(const void *k, gbt_subtype_context *cxt) \
{ \
	return fabs((float8) *(const key_ctype *) k - (float8) get_sub(cxt->query)); \
}


/*
 *	Numeric btree functions
 */



/*
 * Note: The factor 0.49 in following macro avoids floating point overflows
 */
#define penalty_num(result,olower,oupper,nlower,nupper) do { \
  double	tmp = 0.0F; \
  (*(result))	= 0.0F; \
  if ( (nupper) > (oupper) ) \
	  tmp += ( ((double)nupper)*0.49F - ((double)oupper)*0.49F ); \
  if (	(olower) > (nlower)  ) \
	  tmp += ( ((double)olower)*0.49F - ((double)nlower)*0.49F ); \
  if (tmp > 0.0F) \
  { \
	(*(result)) += FLT_MIN; \
	(*(result)) += (float) ( ((double)(tmp)) / ( (double)(tmp) + ( ((double)(oupper))*0.49F - ((double)(olower))*0.49F ) ) ); \
	(*(result)) *= (FLT_MAX / (((GISTENTRY *) PG_GETARG_POINTER(0))->rel->rd_att->natts + 1)); \
  } \
} while (0)


/*
 * Convert an Interval to an approximate equivalent number of seconds
 * (as a double).  Here because we need it for time/timetz as well as
 * interval.  See interval_cmp_internal for comparison.
 */
#define INTERVAL_TO_SEC(ivp) \
	(((double) (ivp)->time) / ((double) USECS_PER_SEC) + \
	 (ivp)->day * (24.0 * SECS_PER_HOUR) + \
	 (ivp)->month * (30.0 * SECS_PER_DAY))

#define GET_FLOAT_DISTANCE(t, arg1, arg2)	fabs( ((float8) *((const t *) (arg1))) - ((float8) *((const t *) (arg2))) )


extern Interval *abs_interval(Interval *a);

extern bool gbt_num_consistent(const GBT_NUMKEY_R *key, const void *query,
							   const StrategyNumber *strategy, bool is_leaf,
							   const gbtree_ninfo *tinfo, FmgrInfo *flinfo);

extern bool gbt_num_consistent_x(const GBT_NUMKEY_R *key, const void *query,
								 Datum queryDatum, Oid subtype, Oid collation,
								 const StrategyNumber *strategy, bool is_leaf,
								 const gbtree_ninfo *tinfo, FmgrInfo *flinfo);

extern float8 gbt_num_distance(const GBT_NUMKEY_R *key, const void *query,
							   bool is_leaf, const gbtree_ninfo *tinfo, FmgrInfo *flinfo);

extern float8 gbt_num_distance_x(const GBT_NUMKEY_R *key, const void *query,
								 Datum queryDatum, Oid subtype, Oid collation,
								 bool is_leaf,
								 const gbtree_ninfo *tinfo, FmgrInfo *flinfo);

extern GIST_SPLITVEC *gbt_num_picksplit(const GistEntryVector *entryvec, GIST_SPLITVEC *v,
										const gbtree_ninfo *tinfo, FmgrInfo *flinfo);

extern GISTENTRY *gbt_num_compress(GISTENTRY *entry, const gbtree_ninfo *tinfo);

extern GISTENTRY *gbt_num_fetch(GISTENTRY *entry, const gbtree_ninfo *tinfo);

extern void *gbt_num_union(GBT_NUMKEY *out, const GistEntryVector *entryvec,
						   const gbtree_ninfo *tinfo, FmgrInfo *flinfo);

extern bool gbt_num_same(const GBT_NUMKEY *a, const GBT_NUMKEY *b,
						 const gbtree_ninfo *tinfo, FmgrInfo *flinfo);

extern void gbt_num_bin_union(Datum *u, GBT_NUMKEY *e,
							  const gbtree_ninfo *tinfo, FmgrInfo *flinfo);

#endif
