/*
 * the PLyBackgroundSession class
 *
 * src/pl/plpython/plpy_bgsession.c
 */

#include "postgres.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "parser/parse_type.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#include "plpython.h"

#include "plpy_bgsession.h"

#include "plpy_elog.h"
#include "plpy_main.h"
#include "plpy_planobject.h"
#include "plpy_spi.h"


static PyObject *PLyBackgroundSession_new(PyTypeObject *type, PyObject *args, PyObject *kw);
static void PLyBackgroundSession_dealloc(PyObject *subxact);
static PyObject *PLyBackgroundSession_enter(PyObject *self, PyObject *unused);
static PyObject *PLyBackgroundSession_close(PyObject *self, PyObject *args);
static PyObject *PLyBackgroundSession_execute(PyObject *self, PyObject *args);
static PyObject *PLyBackgroundSession_prepare(PyObject *self, PyObject *args);
static PyObject *PLyBackgroundSession_execute_prepared(PyObject *self, PyObject *args);

static char PLyBackgroundSession_doc[] = {
	"PostgreSQL background session context manager"
};

static PyMethodDef PLyBackgroundSession_methods[] = {
	{"close", PLyBackgroundSession_close, METH_VARARGS, NULL},
	{"__enter__", PLyBackgroundSession_enter, METH_VARARGS, NULL},
	{"__exit__", PLyBackgroundSession_close, METH_VARARGS, NULL},
	{"execute", PLyBackgroundSession_execute, METH_VARARGS, NULL},
	{"prepare", PLyBackgroundSession_prepare, METH_VARARGS, NULL},
	{"execute_prepared", PLyBackgroundSession_execute_prepared, METH_VARARGS, NULL},
	{NULL, NULL, 0, NULL}
};

static PyTypeObject PLyBackgroundSession_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"plpy.BackgroundSession",		/* tp_name */
	sizeof(PLyBackgroundSession_Object),	/* tp_size */
	0,								/* tp_itemsize */

	/*
	 * methods
	 */
	PLyBackgroundSession_dealloc,	/* tp_dealloc */
	0,								/* tp_print */
	0,								/* tp_getattr */
	0,								/* tp_setattr */
	0,								/* tp_compare */
	0,								/* tp_repr */
	0,								/* tp_as_number */
	0,								/* tp_as_sequence */
	0,								/* tp_as_mapping */
	0,								/* tp_hash */
	0,								/* tp_call */
	0,								/* tp_str */
	0,								/* tp_getattro */
	0,								/* tp_setattro */
	0,								/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,	/* tp_flags */
	PLyBackgroundSession_doc,		/* tp_doc */
	0,								/* tp_traverse */
	0,								/* tp_clear */
	0,								/* tp_richcompare */
	0,								/* tp_weaklistoffset */
	0,								/* tp_iter */
	0,								/* tp_iternext */
	PLyBackgroundSession_methods,	/* tp_tpmethods */
	0,								/* tp_members */
	0,								/* tp_getset */
	0,								/* tp_base */
	0,								/* tp_dict */
	0,								/* tp_descr_get */
	0,								/* tp_descr_set */
	0,								/* tp_dictoffset */
	0,								/* tp_init */
	0,								/* tp_alloc */
	PLyBackgroundSession_new,		/* tp_new */
	0,								/* tp_free */
};


int
PLy_bgsession_init_type(PyObject *module)
{
	if (PyType_Ready(&PLyBackgroundSession_Type) < 0)
		return -1;

	Py_INCREF(&PLyBackgroundSession_Type);
	if (PyModule_AddObject(module, "BackgroundSession", (PyObject *)&PLyBackgroundSession_Type) < 0)
		return -1;

	return 0;
}

static PyObject *
PLyBackgroundSession_new(PyTypeObject *type, PyObject *args, PyObject *kw)
{
	PyObject   *result = type->tp_alloc(type, 0);
	PLyBackgroundSession_Object *bgsession = (PLyBackgroundSession_Object *) result;

	bgsession->bgsession = BackgroundSessionStart();

	return result;
}

/*
 * Python requires a dealloc function to be defined
 */
static void
PLyBackgroundSession_dealloc(PyObject *self)
{
}

/*
 * bgsession.__enter__() or bgsession.enter()
 */
static PyObject *
PLyBackgroundSession_enter(PyObject *self, PyObject *unused)
{
	Py_INCREF(self);
	return self;
}

/*
 * bgsession.close() or bgsession.__exit__(exc_type, exc, tb)
 */
static PyObject *
PLyBackgroundSession_close(PyObject *self, PyObject *args)
{
	PLyBackgroundSession_Object *bgsession = (PLyBackgroundSession_Object *) self;

	if (!bgsession->bgsession)
	{
		PLy_exception_set(PyExc_ValueError, "this background session has already been closed");
		return NULL;
	}

	BackgroundSessionEnd(bgsession->bgsession);
	bgsession->bgsession = NULL;

	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *
PLyBackgroundSession_execute(PyObject *self, PyObject *args)
{
	PLyBackgroundSession_Object *bgsession = (PLyBackgroundSession_Object *) self;
	char	   *query;

	if (!bgsession->bgsession)
	{
		PLy_exception_set(PyExc_ValueError, "this background session has already been closed");
		return NULL;
	}

	if (PyArg_ParseTuple(args, "s:execute", &query))
	{
		BackgroundSessionResult *result;
		HeapTuple  *tuples;
		ListCell   *lc;
		int			i;
		SPITupleTable faketupletable;

		result = BackgroundSessionExecute(bgsession->bgsession, query);
		if (result->tupdesc)
		{
			tuples = palloc(list_length(result->tuples) * sizeof(*tuples));
			i = 0;
			foreach (lc, result->tuples)
			{
				HeapTuple tuple = (HeapTuple) lfirst(lc);
				tuples[i++] = tuple;
			}
			faketupletable.tupdesc = result->tupdesc;
			faketupletable.vals = tuples;
			return PLy_spi_execute_fetch_result(&faketupletable, list_length(result->tuples), SPI_OK_SELECT);
		}
		else
			return PLy_spi_execute_fetch_result(NULL, 0, SPI_OK_UTILITY);
	}
	else
		PLy_exception_set(PLy_exc_error, "background session execute expected a query");
	return NULL;
}

// XXX lots of overlap with PLy_spi_prepare
static PyObject *
PLyBackgroundSession_prepare(PyObject *self, PyObject *args)
{
	PLyBackgroundSession_Object *bgsession = (PLyBackgroundSession_Object *) self;
	char	   *query;
	PyObject   *paraminfo = NULL;
	BackgroundSessionPreparedStatement *bgstmt;
	int			nargs = 0;
	const char **argnames = NULL;
	PLyPlanObject *plan;
	PyObject   *volatile optr = NULL;
	volatile MemoryContext oldcontext;
	int			i;
	PLyExecutionContext *exec_ctx = PLy_current_execution_context();
	PyObject *keys;

	if (!bgsession->bgsession)
	{
		PLy_exception_set(PyExc_ValueError, "this background session has already been closed");
		return NULL;
	}

	if (!PyArg_ParseTuple(args, "s|O:prepare", &query, &paraminfo))
		return NULL;

	if (paraminfo &&
		!PySequence_Check(paraminfo) && !PyMapping_Check(paraminfo))
	{
		PLy_exception_set(PyExc_TypeError,
						  "second argument of prepare must be a sequence or mapping");
		return NULL;
	}

	if ((plan = (PLyPlanObject *) PLy_plan_new()) == NULL)
		return NULL;

	plan->mcxt = AllocSetContextCreate(TopMemoryContext,
									   "PL/Python background plan context",
									   ALLOCSET_DEFAULT_MINSIZE,
									   ALLOCSET_DEFAULT_INITSIZE,
									   ALLOCSET_DEFAULT_MAXSIZE);

	oldcontext = MemoryContextSwitchTo(plan->mcxt);

	if (!paraminfo)
		nargs = 0;
	else if (PySequence_Check(paraminfo))
		nargs = PySequence_Length(paraminfo);
	else
		nargs = PyMapping_Length(paraminfo);

	plan->nargs = nargs;
	plan->types = nargs ? palloc(sizeof(Oid) * nargs) : NULL;
	plan->values = nargs ? palloc(sizeof(Datum) * nargs) : NULL;
	plan->args = nargs ? palloc(sizeof(PLyTypeInfo) * nargs) : NULL;

	MemoryContextSwitchTo(oldcontext);

	if (PyMapping_Check(paraminfo))
	{
		argnames = palloc(nargs * sizeof(char *));
		keys = PyMapping_Keys(paraminfo);
	}
	else
	{
		argnames = NULL;
		keys = NULL;
	}

	for (i = 0; i < nargs; i++)
	{
		PLy_typeinfo_init(&plan->args[i], plan->mcxt);
		plan->values[i] = PointerGetDatum(NULL);
	}

	for (i = 0; i < nargs; i++)
	{
		char	   *sptr;
		HeapTuple	typeTup;
		Oid			typeId;
		int32		typmod;

		if (keys)
		{
			PyObject *key;
			char *keystr;

			key = PySequence_GetItem(keys, i);
			argnames[i] = keystr = PyString_AsString(key);
			optr = PyMapping_GetItemString(paraminfo, keystr);
			Py_DECREF(key);
		}
		else
			optr = PySequence_GetItem(paraminfo, i);

		if (PyString_Check(optr))
			sptr = PyString_AsString(optr);
		else if (PyUnicode_Check(optr))
			sptr = PLyUnicode_AsString(optr);
		else
		{
			ereport(ERROR,
					(errmsg("background session prepare: type name at ordinal position %d is not a string", i)));
			sptr = NULL;	/* keep compiler quiet */
		}

		/********************************************************
		 * Resolve argument type names and then look them up by
		 * oid in the system cache, and remember the required
		 *information for input conversion.
		 ********************************************************/

		parseTypeString(sptr, &typeId, &typmod, false);

		typeTup = SearchSysCache1(TYPEOID,
								  ObjectIdGetDatum(typeId));
		if (!HeapTupleIsValid(typeTup))
			elog(ERROR, "cache lookup failed for type %u", typeId);

		Py_DECREF(optr);

		/*
		 * set optr to NULL, so we won't try to unref it again in case of
		 * an error
		 */
		optr = NULL;

		plan->types[i] = typeId;
		PLy_output_datum_func(&plan->args[i], typeTup, exec_ctx->curr_proc->langid, exec_ctx->curr_proc->trftypes);
		ReleaseSysCache(typeTup);
	}

	bgstmt = BackgroundSessionPrepare(bgsession->bgsession, query, nargs, plan->types, argnames);

	plan->bgstmt = bgstmt;

	return (PyObject *) plan;
}

static PyObject *
PLyBackgroundSession_execute_prepared(PyObject *self, PyObject *args)
{
	PLyBackgroundSession_Object *bgsession pg_attribute_unused() = (PLyBackgroundSession_Object *) self;
	PyObject   *ob;
	PLyPlanObject *plan;
	PyObject   *list = NULL;
	int			nargs;
	bool	   *nulls;
	BackgroundSessionResult *result;
	HeapTuple  *tuples;
	ListCell   *lc;
	int			i;
	SPITupleTable faketupletable;

	if (!bgsession->bgsession)
	{
		PLy_exception_set(PyExc_ValueError, "this background session has already been closed");
		return NULL;
	}

	if (!PyArg_ParseTuple(args, "O|O:execute_prepared", &ob, &list))
		return NULL;

	if (!is_PLyPlanObject(ob))
	{
		PLy_exception_set(PyExc_TypeError,
						  "first argument of execute_prepared must be a plan");
		return NULL;
	}

	plan = (PLyPlanObject *) ob;

	if (list && (!PySequence_Check(list)))
	{
		PLy_exception_set(PyExc_TypeError,
						  "second argument of execute_prepared must be a sequence");
		return NULL;
	}

	nargs = list ? PySequence_Length(list) : 0;

	if (nargs != plan->nargs)
	{
		char	   *sv;
		PyObject   *so = PyObject_Str(list);

		if (!so)
			PLy_elog(ERROR, "could not execute plan");
		sv = PyString_AsString(so);
		PLy_exception_set_plural(PyExc_TypeError,
							  "Expected sequence of %d argument, got %d: %s",
							 "Expected sequence of %d arguments, got %d: %s",
								 plan->nargs,
								 plan->nargs, nargs, sv);
		Py_DECREF(so);

		return NULL;
	}

	nulls = palloc(nargs * sizeof(*nulls));

	for (i = 0; i < nargs; i++)
	{
		PyObject   *elem;

		elem = PySequence_GetItem(list, i);
		if (elem != Py_None)
		{
			PG_TRY();
			{
				plan->values[i] =
					plan->args[i].out.d.func(&(plan->args[i].out.d),
											 -1,
											 elem,
											 false);
			}
			PG_CATCH();
			{
				Py_DECREF(elem);
				PG_RE_THROW();
			}
			PG_END_TRY();

			Py_DECREF(elem);
			nulls[i] = false;
		}
		else
		{
			Py_DECREF(elem);
			plan->values[i] =
				InputFunctionCall(&(plan->args[i].out.d.typfunc),
								  NULL,
								  plan->args[i].out.d.typioparam,
								  -1);
			nulls[i] = true;
		}
	}

	result = BackgroundSessionExecutePrepared(plan->bgstmt, nargs, plan->values, nulls);
	if (result->tupdesc)
	{
		tuples = palloc(list_length(result->tuples) * sizeof(*tuples));
		i = 0;
		foreach (lc, result->tuples)
		{
			HeapTuple tuple = (HeapTuple) lfirst(lc);
			tuples[i++] = tuple;
		}
		faketupletable.tupdesc = result->tupdesc;
		faketupletable.vals = tuples;
		return PLy_spi_execute_fetch_result(&faketupletable, list_length(result->tuples), SPI_OK_SELECT);
	}
	else
		return PLy_spi_execute_fetch_result(NULL, 0, SPI_OK_UTILITY);
}
