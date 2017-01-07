/*
 * src/pl/plpython/plpy_bgsession.h
 */

#ifndef PLPY_BGSESSION_H
#define PLPY_BGSESSION_H

#include "tcop/bgsession.h"

typedef struct PLyBackgroundSession_Object
{
	PyObject_HEAD
	BackgroundSession *bgsession;
} PLyBackgroundSession_Object;

extern int PLy_bgsession_init_type(PyObject *module);

#endif   /* PLPY_BGSESSION_H */
