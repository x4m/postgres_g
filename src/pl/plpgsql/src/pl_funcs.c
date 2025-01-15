/*-------------------------------------------------------------------------
 *
 * pl_funcs.c		- Misc functions for the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_funcs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "plpgsql.h"
#include "utils/memutils.h"

/* ----------
 * Local variables for namespace handling
 *
 * The namespace structure actually forms a tree, of which only one linear
 * list or "chain" (from the youngest item to the root) is accessible from
 * any one plpgsql statement.  During initial parsing of a function, ns_top
 * points to the youngest item accessible from the block currently being
 * parsed.  We store the entire tree, however, since at runtime we will need
 * to access the chain that's relevant to any one statement.
 *
 * Block boundaries in the namespace chain are marked by PLPGSQL_NSTYPE_LABEL
 * items.
 * ----------
 */
static PLpgSQL_nsitem *ns_top = NULL;


/* ----------
 * plpgsql_ns_init			Initialize namespace processing for a new function
 * ----------
 */
void
plpgsql_ns_init(void)
{
	ns_top = NULL;
}


/* ----------
 * plpgsql_ns_push			Create a new namespace level
 * ----------
 */
void
plpgsql_ns_push(const char *label, PLpgSQL_label_type label_type)
{
	if (label == NULL)
		label = "";
	plpgsql_ns_additem(PLPGSQL_NSTYPE_LABEL, (int) label_type, label);
}


/* ----------
 * plpgsql_ns_pop			Pop entries back to (and including) the last label
 * ----------
 */
void
plpgsql_ns_pop(void)
{
	Assert(ns_top != NULL);
	while (ns_top->itemtype != PLPGSQL_NSTYPE_LABEL)
		ns_top = ns_top->prev;
	ns_top = ns_top->prev;
}


/* ----------
 * plpgsql_ns_top			Fetch the current namespace chain end
 * ----------
 */
PLpgSQL_nsitem *
plpgsql_ns_top(void)
{
	return ns_top;
}


/* ----------
 * plpgsql_ns_additem		Add an item to the current namespace chain
 * ----------
 */
void
plpgsql_ns_additem(PLpgSQL_nsitem_type itemtype, int itemno, const char *name)
{
	PLpgSQL_nsitem *nse;

	Assert(name != NULL);
	/* first item added must be a label */
	Assert(ns_top != NULL || itemtype == PLPGSQL_NSTYPE_LABEL);

	nse = palloc(offsetof(PLpgSQL_nsitem, name) + strlen(name) + 1);
	nse->itemtype = itemtype;
	nse->itemno = itemno;
	nse->prev = ns_top;
	strcpy(nse->name, name);
	ns_top = nse;
}


/* ----------
 * plpgsql_ns_lookup		Lookup an identifier in the given namespace chain
 *
 * Note that this only searches for variables, not labels.
 *
 * If localmode is true, only the topmost block level is searched.
 *
 * name1 must be non-NULL.  Pass NULL for name2 and/or name3 if parsing a name
 * with fewer than three components.
 *
 * If names_used isn't NULL, *names_used receives the number of names
 * matched: 0 if no match, 1 if name1 matched an unqualified variable name,
 * 2 if name1 and name2 matched a block label + variable name.
 *
 * Note that name3 is never directly matched to anything.  However, if it
 * isn't NULL, we will disregard qualified matches to scalar variables.
 * Similarly, if name2 isn't NULL, we disregard unqualified matches to
 * scalar variables.
 * ----------
 */
PLpgSQL_nsitem *
plpgsql_ns_lookup(PLpgSQL_nsitem *ns_cur, bool localmode,
				  const char *name1, const char *name2, const char *name3,
				  int *names_used)
{
	/* Outer loop iterates once per block level in the namespace chain */
	while (ns_cur != NULL)
	{
		PLpgSQL_nsitem *nsitem;

		/* Check this level for unqualified match to variable name */
		for (nsitem = ns_cur;
			 nsitem->itemtype != PLPGSQL_NSTYPE_LABEL;
			 nsitem = nsitem->prev)
		{
			if (strcmp(nsitem->name, name1) == 0)
			{
				if (name2 == NULL ||
					nsitem->itemtype != PLPGSQL_NSTYPE_VAR)
				{
					if (names_used)
						*names_used = 1;
					return nsitem;
				}
			}
		}

		/* Check this level for qualified match to variable name */
		if (name2 != NULL &&
			strcmp(nsitem->name, name1) == 0)
		{
			for (nsitem = ns_cur;
				 nsitem->itemtype != PLPGSQL_NSTYPE_LABEL;
				 nsitem = nsitem->prev)
			{
				if (strcmp(nsitem->name, name2) == 0)
				{
					if (name3 == NULL ||
						nsitem->itemtype != PLPGSQL_NSTYPE_VAR)
					{
						if (names_used)
							*names_used = 2;
						return nsitem;
					}
				}
			}
		}

		if (localmode)
			break;				/* do not look into upper levels */

		ns_cur = nsitem->prev;
	}

	/* This is just to suppress possibly-uninitialized-variable warnings */
	if (names_used)
		*names_used = 0;
	return NULL;				/* No match found */
}


/* ----------
 * plpgsql_ns_lookup_label		Lookup a label in the given namespace chain
 * ----------
 */
PLpgSQL_nsitem *
plpgsql_ns_lookup_label(PLpgSQL_nsitem *ns_cur, const char *name)
{
	while (ns_cur != NULL)
	{
		if (ns_cur->itemtype == PLPGSQL_NSTYPE_LABEL &&
			strcmp(ns_cur->name, name) == 0)
			return ns_cur;
		ns_cur = ns_cur->prev;
	}

	return NULL;				/* label not found */
}


/* ----------
 * plpgsql_ns_find_nearest_loop		Find innermost loop label in namespace chain
 * ----------
 */
PLpgSQL_nsitem *
plpgsql_ns_find_nearest_loop(PLpgSQL_nsitem *ns_cur)
{
	while (ns_cur != NULL)
	{
		if (ns_cur->itemtype == PLPGSQL_NSTYPE_LABEL &&
			ns_cur->itemno == PLPGSQL_LABEL_LOOP)
			return ns_cur;
		ns_cur = ns_cur->prev;
	}

	return NULL;				/* no loop found */
}


/*
 * Statement type as a string, for use in error messages etc.
 */
const char *
plpgsql_stmt_typename(PLpgSQL_stmt *stmt)
{
	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			return _("statement block");
		case PLPGSQL_STMT_ASSIGN:
			return _("assignment");
		case PLPGSQL_STMT_IF:
			return "IF";
		case PLPGSQL_STMT_CASE:
			return "CASE";
		case PLPGSQL_STMT_LOOP:
			return "LOOP";
		case PLPGSQL_STMT_WHILE:
			return "WHILE";
		case PLPGSQL_STMT_FORI:
			return _("FOR with integer loop variable");
		case PLPGSQL_STMT_FORS:
			return _("FOR over SELECT rows");
		case PLPGSQL_STMT_FORC:
			return _("FOR over cursor");
		case PLPGSQL_STMT_FOREACH_A:
			return _("FOREACH over array");
		case PLPGSQL_STMT_EXIT:
			return ((PLpgSQL_stmt_exit *) stmt)->is_exit ? "EXIT" : "CONTINUE";
		case PLPGSQL_STMT_RETURN:
			return "RETURN";
		case PLPGSQL_STMT_RETURN_NEXT:
			return "RETURN NEXT";
		case PLPGSQL_STMT_RETURN_QUERY:
			return "RETURN QUERY";
		case PLPGSQL_STMT_RAISE:
			return "RAISE";
		case PLPGSQL_STMT_ASSERT:
			return "ASSERT";
		case PLPGSQL_STMT_EXECSQL:
			return _("SQL statement");
		case PLPGSQL_STMT_DYNEXECUTE:
			return "EXECUTE";
		case PLPGSQL_STMT_DYNFORS:
			return _("FOR over EXECUTE statement");
		case PLPGSQL_STMT_GETDIAG:
			return ((PLpgSQL_stmt_getdiag *) stmt)->is_stacked ?
				"GET STACKED DIAGNOSTICS" : "GET DIAGNOSTICS";
		case PLPGSQL_STMT_OPEN:
			return "OPEN";
		case PLPGSQL_STMT_FETCH:
			return ((PLpgSQL_stmt_fetch *) stmt)->is_move ? "MOVE" : "FETCH";
		case PLPGSQL_STMT_CLOSE:
			return "CLOSE";
		case PLPGSQL_STMT_PERFORM:
			return "PERFORM";
		case PLPGSQL_STMT_CALL:
			return ((PLpgSQL_stmt_call *) stmt)->is_call ? "CALL" : "DO";
		case PLPGSQL_STMT_COMMIT:
			return "COMMIT";
		case PLPGSQL_STMT_ROLLBACK:
			return "ROLLBACK";
	}

	return "unknown";
}

/*
 * GET DIAGNOSTICS item name as a string, for use in error messages etc.
 */
const char *
plpgsql_getdiag_kindname(PLpgSQL_getdiag_kind kind)
{
	switch (kind)
	{
		case PLPGSQL_GETDIAG_ROW_COUNT:
			return "ROW_COUNT";
		case PLPGSQL_GETDIAG_ROUTINE_OID:
			return "PG_ROUTINE_OID";
		case PLPGSQL_GETDIAG_CONTEXT:
			return "PG_CONTEXT";
		case PLPGSQL_GETDIAG_ERROR_CONTEXT:
			return "PG_EXCEPTION_CONTEXT";
		case PLPGSQL_GETDIAG_ERROR_DETAIL:
			return "PG_EXCEPTION_DETAIL";
		case PLPGSQL_GETDIAG_ERROR_HINT:
			return "PG_EXCEPTION_HINT";
		case PLPGSQL_GETDIAG_RETURNED_SQLSTATE:
			return "RETURNED_SQLSTATE";
		case PLPGSQL_GETDIAG_COLUMN_NAME:
			return "COLUMN_NAME";
		case PLPGSQL_GETDIAG_CONSTRAINT_NAME:
			return "CONSTRAINT_NAME";
		case PLPGSQL_GETDIAG_DATATYPE_NAME:
			return "PG_DATATYPE_NAME";
		case PLPGSQL_GETDIAG_MESSAGE_TEXT:
			return "MESSAGE_TEXT";
		case PLPGSQL_GETDIAG_TABLE_NAME:
			return "TABLE_NAME";
		case PLPGSQL_GETDIAG_SCHEMA_NAME:
			return "SCHEMA_NAME";
	}

	return "unknown";
}


/**********************************************************************
 * Mark assignment source expressions that have local target variables,
 * that is, variables declared within the exception block most closely
 * containing the assignment itself.  (Such target variables need not be
 * preserved if the assignment's source expression raises an error,
 * allowing better optimization.)
 *
 * This code need not be called if the plpgsql function contains no exception
 * blocks, because expr_is_assignment_source() will have set all the flags
 * to true already.  Also, we need not examine default-value expressions for
 * variables, because variable declarations are necessarily within the nearest
 * exception block.  (In DECLARE ... BEGIN ... EXCEPTION ... END, the variable
 * initializations are done before entering the exception scope.)  So it's
 * sufficient to find assignment statements.
 *
 * Within the recursion, local_dnos is a Bitmapset of dnos of variables
 * known to be declared within the current exception level.
 **********************************************************************/
static void mark_stmt(PLpgSQL_stmt *stmt, Bitmapset *local_dnos);
static void mark_block(PLpgSQL_stmt_block *block, Bitmapset *local_dnos);
static void mark_assign(PLpgSQL_stmt_assign *stmt, Bitmapset *local_dnos);
static void mark_if(PLpgSQL_stmt_if *stmt, Bitmapset *local_dnos);
static void mark_case(PLpgSQL_stmt_case *stmt, Bitmapset *local_dnos);
static void mark_loop(PLpgSQL_stmt_loop *stmt, Bitmapset *local_dnos);
static void mark_while(PLpgSQL_stmt_while *stmt, Bitmapset *local_dnos);
static void mark_fori(PLpgSQL_stmt_fori *stmt, Bitmapset *local_dnos);
static void mark_fors(PLpgSQL_stmt_fors *stmt, Bitmapset *local_dnos);
static void mark_forc(PLpgSQL_stmt_forc *stmt, Bitmapset *local_dnos);
static void mark_foreach_a(PLpgSQL_stmt_foreach_a *stmt, Bitmapset *local_dnos);
static void mark_exit(PLpgSQL_stmt_exit *stmt, Bitmapset *local_dnos);
static void mark_return(PLpgSQL_stmt_return *stmt, Bitmapset *local_dnos);
static void mark_return_next(PLpgSQL_stmt_return_next *stmt, Bitmapset *local_dnos);
static void mark_return_query(PLpgSQL_stmt_return_query *stmt, Bitmapset *local_dnos);
static void mark_raise(PLpgSQL_stmt_raise *stmt, Bitmapset *local_dnos);
static void mark_assert(PLpgSQL_stmt_assert *stmt, Bitmapset *local_dnos);
static void mark_execsql(PLpgSQL_stmt_execsql *stmt, Bitmapset *local_dnos);
static void mark_dynexecute(PLpgSQL_stmt_dynexecute *stmt, Bitmapset *local_dnos);
static void mark_dynfors(PLpgSQL_stmt_dynfors *stmt, Bitmapset *local_dnos);
static void mark_getdiag(PLpgSQL_stmt_getdiag *stmt, Bitmapset *local_dnos);
static void mark_open(PLpgSQL_stmt_open *stmt, Bitmapset *local_dnos);
static void mark_fetch(PLpgSQL_stmt_fetch *stmt, Bitmapset *local_dnos);
static void mark_close(PLpgSQL_stmt_close *stmt, Bitmapset *local_dnos);
static void mark_perform(PLpgSQL_stmt_perform *stmt, Bitmapset *local_dnos);
static void mark_call(PLpgSQL_stmt_call *stmt, Bitmapset *local_dnos);
static void mark_commit(PLpgSQL_stmt_commit *stmt, Bitmapset *local_dnos);
static void mark_rollback(PLpgSQL_stmt_rollback *stmt, Bitmapset *local_dnos);


static void
mark_stmt(PLpgSQL_stmt *stmt, Bitmapset *local_dnos)
{
	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			mark_block((PLpgSQL_stmt_block *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_ASSIGN:
			mark_assign((PLpgSQL_stmt_assign *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_IF:
			mark_if((PLpgSQL_stmt_if *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_CASE:
			mark_case((PLpgSQL_stmt_case *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_LOOP:
			mark_loop((PLpgSQL_stmt_loop *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_WHILE:
			mark_while((PLpgSQL_stmt_while *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_FORI:
			mark_fori((PLpgSQL_stmt_fori *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_FORS:
			mark_fors((PLpgSQL_stmt_fors *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_FORC:
			mark_forc((PLpgSQL_stmt_forc *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_FOREACH_A:
			mark_foreach_a((PLpgSQL_stmt_foreach_a *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_EXIT:
			mark_exit((PLpgSQL_stmt_exit *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_RETURN:
			mark_return((PLpgSQL_stmt_return *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_RETURN_NEXT:
			mark_return_next((PLpgSQL_stmt_return_next *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_RETURN_QUERY:
			mark_return_query((PLpgSQL_stmt_return_query *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_RAISE:
			mark_raise((PLpgSQL_stmt_raise *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_ASSERT:
			mark_assert((PLpgSQL_stmt_assert *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_EXECSQL:
			mark_execsql((PLpgSQL_stmt_execsql *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_DYNEXECUTE:
			mark_dynexecute((PLpgSQL_stmt_dynexecute *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_DYNFORS:
			mark_dynfors((PLpgSQL_stmt_dynfors *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_GETDIAG:
			mark_getdiag((PLpgSQL_stmt_getdiag *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_OPEN:
			mark_open((PLpgSQL_stmt_open *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_FETCH:
			mark_fetch((PLpgSQL_stmt_fetch *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_CLOSE:
			mark_close((PLpgSQL_stmt_close *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_PERFORM:
			mark_perform((PLpgSQL_stmt_perform *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_CALL:
			mark_call((PLpgSQL_stmt_call *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_COMMIT:
			mark_commit((PLpgSQL_stmt_commit *) stmt, local_dnos);
			break;
		case PLPGSQL_STMT_ROLLBACK:
			mark_rollback((PLpgSQL_stmt_rollback *) stmt, local_dnos);
			break;
		default:
			elog(ERROR, "unrecognized cmd_type: %d", stmt->cmd_type);
			break;
	}
}

static void
mark_stmts(List *stmts, Bitmapset *local_dnos)
{
	ListCell   *s;

	foreach(s, stmts)
	{
		mark_stmt((PLpgSQL_stmt *) lfirst(s), local_dnos);
	}
}

static void
mark_block(PLpgSQL_stmt_block *block, Bitmapset *local_dnos)
{
	if (block->exceptions)
	{
		ListCell   *e;

		/*
		 * The block creates a new exception scope, so variables declared at
		 * outer levels are nonlocal.  For that matter, so are any variables
		 * declared in the block's DECLARE section.  Hence, we must pass down
		 * empty local_dnos.
		 */
		mark_stmts(block->body, NULL);

		foreach(e, block->exceptions->exc_list)
		{
			PLpgSQL_exception *exc = (PLpgSQL_exception *) lfirst(e);

			mark_stmts(exc->action, NULL);
		}
	}
	else
	{
		/*
		 * Otherwise, the block does not create a new exception scope, and any
		 * variables it declares can also be considered local within it.  Note
		 * that only initializable datum types (VAR, REC) are included in
		 * initvarnos; but that's sufficient for our purposes.
		 */
		local_dnos = bms_copy(local_dnos);
		for (int i = 0; i < block->n_initvars; i++)
			local_dnos = bms_add_member(local_dnos, block->initvarnos[i]);
		mark_stmts(block->body, local_dnos);
		bms_free(local_dnos);
	}
}

static void
mark_assign(PLpgSQL_stmt_assign *stmt, Bitmapset *local_dnos)
{
	PLpgSQL_expr *expr = stmt->expr;

	/*
	 * If the assignment target is a plain DTYPE_VAR datum, mark it as local
	 * or not.  (If it's not a VAR, we don't care.)
	 */
	if (expr->target_param >= 0)
		expr->target_is_local = bms_is_member(expr->target_param, local_dnos);
}

static void
mark_if(PLpgSQL_stmt_if *stmt, Bitmapset *local_dnos)
{
	ListCell   *l;

	/* stmt->cond cannot be an assignment source */
	mark_stmts(stmt->then_body, local_dnos);
	foreach(l, stmt->elsif_list)
	{
		PLpgSQL_if_elsif *elif = (PLpgSQL_if_elsif *) lfirst(l);

		/* elif->cond cannot be an assignment source */
		mark_stmts(elif->stmts, local_dnos);
	}
	mark_stmts(stmt->else_body, local_dnos);
}

static void
mark_case(PLpgSQL_stmt_case *stmt, Bitmapset *local_dnos)
{
	ListCell   *l;

	/* stmt->t_expr cannot be an assignment source */
	foreach(l, stmt->case_when_list)
	{
		PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);

		/* cwt->expr cannot be an assignment source */
		mark_stmts(cwt->stmts, local_dnos);
	}
	mark_stmts(stmt->else_stmts, local_dnos);
}

static void
mark_loop(PLpgSQL_stmt_loop *stmt, Bitmapset *local_dnos)
{
	mark_stmts(stmt->body, local_dnos);
}

static void
mark_while(PLpgSQL_stmt_while *stmt, Bitmapset *local_dnos)
{
	/* stmt->cond cannot be an assignment source */
	mark_stmts(stmt->body, local_dnos);
}

static void
mark_fori(PLpgSQL_stmt_fori *stmt, Bitmapset *local_dnos)
{
	/* stmt->lower, upper, step cannot be an assignment source */
	mark_stmts(stmt->body, local_dnos);
}

static void
mark_fors(PLpgSQL_stmt_fors *stmt, Bitmapset *local_dnos)
{
	mark_stmts(stmt->body, local_dnos);
	/* stmt->query cannot be an assignment source */
}

static void
mark_forc(PLpgSQL_stmt_forc *stmt, Bitmapset *local_dnos)
{
	mark_stmts(stmt->body, local_dnos);
	/* stmt->argquery cannot be an assignment source */
}

static void
mark_foreach_a(PLpgSQL_stmt_foreach_a *stmt, Bitmapset *local_dnos)
{
	/* stmt->expr cannot be an assignment source */
	mark_stmts(stmt->body, local_dnos);
}

static void
mark_open(PLpgSQL_stmt_open *stmt, Bitmapset *local_dnos)
{
	/* stmt->argquery, query, dynquery cannot be an assignment source */
	/* stmt->params cannot contain an assignment source */
}

static void
mark_fetch(PLpgSQL_stmt_fetch *stmt, Bitmapset *local_dnos)
{
	/* stmt->expr cannot be an assignment source */
}

static void
mark_close(PLpgSQL_stmt_close *stmt, Bitmapset *local_dnos)
{
}

static void
mark_perform(PLpgSQL_stmt_perform *stmt, Bitmapset *local_dnos)
{
	/* stmt->expr cannot be an assignment source */
}

static void
mark_call(PLpgSQL_stmt_call *stmt, Bitmapset *local_dnos)
{
	/* stmt->expr cannot be an assignment source */
}

static void
mark_commit(PLpgSQL_stmt_commit *stmt, Bitmapset *local_dnos)
{
}

static void
mark_rollback(PLpgSQL_stmt_rollback *stmt, Bitmapset *local_dnos)
{
}

static void
mark_exit(PLpgSQL_stmt_exit *stmt, Bitmapset *local_dnos)
{
	/* stmt->cond cannot be an assignment source */
}

static void
mark_return(PLpgSQL_stmt_return *stmt, Bitmapset *local_dnos)
{
	/* stmt->expr cannot be an assignment source */
}

static void
mark_return_next(PLpgSQL_stmt_return_next *stmt, Bitmapset *local_dnos)
{
	/* stmt->expr cannot be an assignment source */
}

static void
mark_return_query(PLpgSQL_stmt_return_query *stmt, Bitmapset *local_dnos)
{
	/* stmt->query, dynquery cannot be an assignment source */
	/* stmt->params cannot contain an assignment source */
}

static void
mark_raise(PLpgSQL_stmt_raise *stmt, Bitmapset *local_dnos)
{
	/* stmt->params cannot contain an assignment source */
	/* stmt->options cannot contain an assignment source */
}

static void
mark_assert(PLpgSQL_stmt_assert *stmt, Bitmapset *local_dnos)
{
	/* stmt->cond, message cannot be an assignment source */
}

static void
mark_execsql(PLpgSQL_stmt_execsql *stmt, Bitmapset *local_dnos)
{
	/* stmt->sqlstmt cannot be an assignment source */
}

static void
mark_dynexecute(PLpgSQL_stmt_dynexecute *stmt, Bitmapset *local_dnos)
{
	/* stmt->query cannot be an assignment source */
	/* stmt->params cannot contain an assignment source */
}

static void
mark_dynfors(PLpgSQL_stmt_dynfors *stmt, Bitmapset *local_dnos)
{
	mark_stmts(stmt->body, local_dnos);
	/* stmt->query cannot be an assignment source */
	/* stmt->params cannot contain an assignment source */
}

static void
mark_getdiag(PLpgSQL_stmt_getdiag *stmt, Bitmapset *local_dnos)
{
}

void
plpgsql_mark_local_assignment_targets(PLpgSQL_function *func)
{
	Bitmapset  *local_dnos;

	/* Function parameters can be treated as local targets at outer level */
	local_dnos = NULL;
	for (int i = 0; i < func->fn_nargs; i++)
		local_dnos = bms_add_member(local_dnos, func->fn_argvarnos[i]);
	if (func->action)
		mark_block(func->action, local_dnos);
	bms_free(local_dnos);
}


/**********************************************************************
 * Release memory when a PL/pgSQL function is no longer needed
 *
 * The code for recursing through the function tree is really only
 * needed to locate PLpgSQL_expr nodes, which may contain references
 * to saved SPI Plans that must be freed.  The function tree itself,
 * along with subsidiary data, is freed in one swoop by freeing the
 * function's permanent memory context.
 **********************************************************************/
static void free_stmt(PLpgSQL_stmt *stmt);
static void free_block(PLpgSQL_stmt_block *block);
static void free_assign(PLpgSQL_stmt_assign *stmt);
static void free_if(PLpgSQL_stmt_if *stmt);
static void free_case(PLpgSQL_stmt_case *stmt);
static void free_loop(PLpgSQL_stmt_loop *stmt);
static void free_while(PLpgSQL_stmt_while *stmt);
static void free_fori(PLpgSQL_stmt_fori *stmt);
static void free_fors(PLpgSQL_stmt_fors *stmt);
static void free_forc(PLpgSQL_stmt_forc *stmt);
static void free_foreach_a(PLpgSQL_stmt_foreach_a *stmt);
static void free_exit(PLpgSQL_stmt_exit *stmt);
static void free_return(PLpgSQL_stmt_return *stmt);
static void free_return_next(PLpgSQL_stmt_return_next *stmt);
static void free_return_query(PLpgSQL_stmt_return_query *stmt);
static void free_raise(PLpgSQL_stmt_raise *stmt);
static void free_assert(PLpgSQL_stmt_assert *stmt);
static void free_execsql(PLpgSQL_stmt_execsql *stmt);
static void free_dynexecute(PLpgSQL_stmt_dynexecute *stmt);
static void free_dynfors(PLpgSQL_stmt_dynfors *stmt);
static void free_getdiag(PLpgSQL_stmt_getdiag *stmt);
static void free_open(PLpgSQL_stmt_open *stmt);
static void free_fetch(PLpgSQL_stmt_fetch *stmt);
static void free_close(PLpgSQL_stmt_close *stmt);
static void free_perform(PLpgSQL_stmt_perform *stmt);
static void free_call(PLpgSQL_stmt_call *stmt);
static void free_commit(PLpgSQL_stmt_commit *stmt);
static void free_rollback(PLpgSQL_stmt_rollback *stmt);
static void free_expr(PLpgSQL_expr *expr);


static void
free_stmt(PLpgSQL_stmt *stmt)
{
	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			free_block((PLpgSQL_stmt_block *) stmt);
			break;
		case PLPGSQL_STMT_ASSIGN:
			free_assign((PLpgSQL_stmt_assign *) stmt);
			break;
		case PLPGSQL_STMT_IF:
			free_if((PLpgSQL_stmt_if *) stmt);
			break;
		case PLPGSQL_STMT_CASE:
			free_case((PLpgSQL_stmt_case *) stmt);
			break;
		case PLPGSQL_STMT_LOOP:
			free_loop((PLpgSQL_stmt_loop *) stmt);
			break;
		case PLPGSQL_STMT_WHILE:
			free_while((PLpgSQL_stmt_while *) stmt);
			break;
		case PLPGSQL_STMT_FORI:
			free_fori((PLpgSQL_stmt_fori *) stmt);
			break;
		case PLPGSQL_STMT_FORS:
			free_fors((PLpgSQL_stmt_fors *) stmt);
			break;
		case PLPGSQL_STMT_FORC:
			free_forc((PLpgSQL_stmt_forc *) stmt);
			break;
		case PLPGSQL_STMT_FOREACH_A:
			free_foreach_a((PLpgSQL_stmt_foreach_a *) stmt);
			break;
		case PLPGSQL_STMT_EXIT:
			free_exit((PLpgSQL_stmt_exit *) stmt);
			break;
		case PLPGSQL_STMT_RETURN:
			free_return((PLpgSQL_stmt_return *) stmt);
			break;
		case PLPGSQL_STMT_RETURN_NEXT:
			free_return_next((PLpgSQL_stmt_return_next *) stmt);
			break;
		case PLPGSQL_STMT_RETURN_QUERY:
			free_return_query((PLpgSQL_stmt_return_query *) stmt);
			break;
		case PLPGSQL_STMT_RAISE:
			free_raise((PLpgSQL_stmt_raise *) stmt);
			break;
		case PLPGSQL_STMT_ASSERT:
			free_assert((PLpgSQL_stmt_assert *) stmt);
			break;
		case PLPGSQL_STMT_EXECSQL:
			free_execsql((PLpgSQL_stmt_execsql *) stmt);
			break;
		case PLPGSQL_STMT_DYNEXECUTE:
			free_dynexecute((PLpgSQL_stmt_dynexecute *) stmt);
			break;
		case PLPGSQL_STMT_DYNFORS:
			free_dynfors((PLpgSQL_stmt_dynfors *) stmt);
			break;
		case PLPGSQL_STMT_GETDIAG:
			free_getdiag((PLpgSQL_stmt_getdiag *) stmt);
			break;
		case PLPGSQL_STMT_OPEN:
			free_open((PLpgSQL_stmt_open *) stmt);
			break;
		case PLPGSQL_STMT_FETCH:
			free_fetch((PLpgSQL_stmt_fetch *) stmt);
			break;
		case PLPGSQL_STMT_CLOSE:
			free_close((PLpgSQL_stmt_close *) stmt);
			break;
		case PLPGSQL_STMT_PERFORM:
			free_perform((PLpgSQL_stmt_perform *) stmt);
			break;
		case PLPGSQL_STMT_CALL:
			free_call((PLpgSQL_stmt_call *) stmt);
			break;
		case PLPGSQL_STMT_COMMIT:
			free_commit((PLpgSQL_stmt_commit *) stmt);
			break;
		case PLPGSQL_STMT_ROLLBACK:
			free_rollback((PLpgSQL_stmt_rollback *) stmt);
			break;
		default:
			elog(ERROR, "unrecognized cmd_type: %d", stmt->cmd_type);
			break;
	}
}

static void
free_stmts(List *stmts)
{
	ListCell   *s;

	foreach(s, stmts)
	{
		free_stmt((PLpgSQL_stmt *) lfirst(s));
	}
}

static void
free_block(PLpgSQL_stmt_block *block)
{
	free_stmts(block->body);
	if (block->exceptions)
	{
		ListCell   *e;

		foreach(e, block->exceptions->exc_list)
		{
			PLpgSQL_exception *exc = (PLpgSQL_exception *) lfirst(e);

			free_stmts(exc->action);
		}
	}
}

static void
free_assign(PLpgSQL_stmt_assign *stmt)
{
	free_expr(stmt->expr);
}

static void
free_if(PLpgSQL_stmt_if *stmt)
{
	ListCell   *l;

	free_expr(stmt->cond);
	free_stmts(stmt->then_body);
	foreach(l, stmt->elsif_list)
	{
		PLpgSQL_if_elsif *elif = (PLpgSQL_if_elsif *) lfirst(l);

		free_expr(elif->cond);
		free_stmts(elif->stmts);
	}
	free_stmts(stmt->else_body);
}

static void
free_case(PLpgSQL_stmt_case *stmt)
{
	ListCell   *l;

	free_expr(stmt->t_expr);
	foreach(l, stmt->case_when_list)
	{
		PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);

		free_expr(cwt->expr);
		free_stmts(cwt->stmts);
	}
	free_stmts(stmt->else_stmts);
}

static void
free_loop(PLpgSQL_stmt_loop *stmt)
{
	free_stmts(stmt->body);
}

static void
free_while(PLpgSQL_stmt_while *stmt)
{
	free_expr(stmt->cond);
	free_stmts(stmt->body);
}

static void
free_fori(PLpgSQL_stmt_fori *stmt)
{
	free_expr(stmt->lower);
	free_expr(stmt->upper);
	free_expr(stmt->step);
	free_stmts(stmt->body);
}

static void
free_fors(PLpgSQL_stmt_fors *stmt)
{
	free_stmts(stmt->body);
	free_expr(stmt->query);
}

static void
free_forc(PLpgSQL_stmt_forc *stmt)
{
	free_stmts(stmt->body);
	free_expr(stmt->argquery);
}

static void
free_foreach_a(PLpgSQL_stmt_foreach_a *stmt)
{
	free_expr(stmt->expr);
	free_stmts(stmt->body);
}

static void
free_open(PLpgSQL_stmt_open *stmt)
{
	ListCell   *lc;

	free_expr(stmt->argquery);
	free_expr(stmt->query);
	free_expr(stmt->dynquery);
	foreach(lc, stmt->params)
	{
		free_expr((PLpgSQL_expr *) lfirst(lc));
	}
}

static void
free_fetch(PLpgSQL_stmt_fetch *stmt)
{
	free_expr(stmt->expr);
}

static void
free_close(PLpgSQL_stmt_close *stmt)
{
}

static void
free_perform(PLpgSQL_stmt_perform *stmt)
{
	free_expr(stmt->expr);
}

static void
free_call(PLpgSQL_stmt_call *stmt)
{
	free_expr(stmt->expr);
}

static void
free_commit(PLpgSQL_stmt_commit *stmt)
{
}

static void
free_rollback(PLpgSQL_stmt_rollback *stmt)
{
}

static void
free_exit(PLpgSQL_stmt_exit *stmt)
{
	free_expr(stmt->cond);
}

static void
free_return(PLpgSQL_stmt_return *stmt)
{
	free_expr(stmt->expr);
}

static void
free_return_next(PLpgSQL_stmt_return_next *stmt)
{
	free_expr(stmt->expr);
}

static void
free_return_query(PLpgSQL_stmt_return_query *stmt)
{
	ListCell   *lc;

	free_expr(stmt->query);
	free_expr(stmt->dynquery);
	foreach(lc, stmt->params)
	{
		free_expr((PLpgSQL_expr *) lfirst(lc));
	}
}

static void
free_raise(PLpgSQL_stmt_raise *stmt)
{
	ListCell   *lc;

	foreach(lc, stmt->params)
	{
		free_expr((PLpgSQL_expr *) lfirst(lc));
	}
	foreach(lc, stmt->options)
	{
		PLpgSQL_raise_option *opt = (PLpgSQL_raise_option *) lfirst(lc);

		free_expr(opt->expr);
	}
}

static void
free_assert(PLpgSQL_stmt_assert *stmt)
{
	free_expr(stmt->cond);
	free_expr(stmt->message);
}

static void
free_execsql(PLpgSQL_stmt_execsql *stmt)
{
	free_expr(stmt->sqlstmt);
}

static void
free_dynexecute(PLpgSQL_stmt_dynexecute *stmt)
{
	ListCell   *lc;

	free_expr(stmt->query);
	foreach(lc, stmt->params)
	{
		free_expr((PLpgSQL_expr *) lfirst(lc));
	}
}

static void
free_dynfors(PLpgSQL_stmt_dynfors *stmt)
{
	ListCell   *lc;

	free_stmts(stmt->body);
	free_expr(stmt->query);
	foreach(lc, stmt->params)
	{
		free_expr((PLpgSQL_expr *) lfirst(lc));
	}
}

static void
free_getdiag(PLpgSQL_stmt_getdiag *stmt)
{
}

static void
free_expr(PLpgSQL_expr *expr)
{
	if (expr && expr->plan)
	{
		SPI_freeplan(expr->plan);
		expr->plan = NULL;
	}
}

void
plpgsql_free_function_memory(PLpgSQL_function *func)
{
	int			i;

	/* Better not call this on an in-use function */
	Assert(func->use_count == 0);

	/* Release plans associated with variable declarations */
	for (i = 0; i < func->ndatums; i++)
	{
		PLpgSQL_datum *d = func->datums[i];

		switch (d->dtype)
		{
			case PLPGSQL_DTYPE_VAR:
			case PLPGSQL_DTYPE_PROMISE:
				{
					PLpgSQL_var *var = (PLpgSQL_var *) d;

					free_expr(var->default_val);
					free_expr(var->cursor_explicit_expr);
				}
				break;
			case PLPGSQL_DTYPE_ROW:
				break;
			case PLPGSQL_DTYPE_REC:
				{
					PLpgSQL_rec *rec = (PLpgSQL_rec *) d;

					free_expr(rec->default_val);
				}
				break;
			case PLPGSQL_DTYPE_RECFIELD:
				break;
			default:
				elog(ERROR, "unrecognized data type: %d", d->dtype);
		}
	}
	func->ndatums = 0;

	/* Release plans in statement tree */
	if (func->action)
		free_block(func->action);
	func->action = NULL;

	/*
	 * And finally, release all memory except the PLpgSQL_function struct
	 * itself (which has to be kept around because there may be multiple
	 * fn_extra pointers to it).
	 */
	if (func->fn_cxt)
		MemoryContextDelete(func->fn_cxt);
	func->fn_cxt = NULL;
}


/**********************************************************************
 * Debug functions for analyzing the compiled code
 **********************************************************************/
static int	dump_indent;

static void dump_ind(void);
static void dump_stmt(PLpgSQL_stmt *stmt);
static void dump_block(PLpgSQL_stmt_block *block);
static void dump_assign(PLpgSQL_stmt_assign *stmt);
static void dump_if(PLpgSQL_stmt_if *stmt);
static void dump_case(PLpgSQL_stmt_case *stmt);
static void dump_loop(PLpgSQL_stmt_loop *stmt);
static void dump_while(PLpgSQL_stmt_while *stmt);
static void dump_fori(PLpgSQL_stmt_fori *stmt);
static void dump_fors(PLpgSQL_stmt_fors *stmt);
static void dump_forc(PLpgSQL_stmt_forc *stmt);
static void dump_foreach_a(PLpgSQL_stmt_foreach_a *stmt);
static void dump_exit(PLpgSQL_stmt_exit *stmt);
static void dump_return(PLpgSQL_stmt_return *stmt);
static void dump_return_next(PLpgSQL_stmt_return_next *stmt);
static void dump_return_query(PLpgSQL_stmt_return_query *stmt);
static void dump_raise(PLpgSQL_stmt_raise *stmt);
static void dump_assert(PLpgSQL_stmt_assert *stmt);
static void dump_execsql(PLpgSQL_stmt_execsql *stmt);
static void dump_dynexecute(PLpgSQL_stmt_dynexecute *stmt);
static void dump_dynfors(PLpgSQL_stmt_dynfors *stmt);
static void dump_getdiag(PLpgSQL_stmt_getdiag *stmt);
static void dump_open(PLpgSQL_stmt_open *stmt);
static void dump_fetch(PLpgSQL_stmt_fetch *stmt);
static void dump_cursor_direction(PLpgSQL_stmt_fetch *stmt);
static void dump_close(PLpgSQL_stmt_close *stmt);
static void dump_perform(PLpgSQL_stmt_perform *stmt);
static void dump_call(PLpgSQL_stmt_call *stmt);
static void dump_commit(PLpgSQL_stmt_commit *stmt);
static void dump_rollback(PLpgSQL_stmt_rollback *stmt);
static void dump_expr(PLpgSQL_expr *expr);


static void
dump_ind(void)
{
	int			i;

	for (i = 0; i < dump_indent; i++)
		printf(" ");
}

static void
dump_stmt(PLpgSQL_stmt *stmt)
{
	printf("%3d:", stmt->lineno);
	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			dump_block((PLpgSQL_stmt_block *) stmt);
			break;
		case PLPGSQL_STMT_ASSIGN:
			dump_assign((PLpgSQL_stmt_assign *) stmt);
			break;
		case PLPGSQL_STMT_IF:
			dump_if((PLpgSQL_stmt_if *) stmt);
			break;
		case PLPGSQL_STMT_CASE:
			dump_case((PLpgSQL_stmt_case *) stmt);
			break;
		case PLPGSQL_STMT_LOOP:
			dump_loop((PLpgSQL_stmt_loop *) stmt);
			break;
		case PLPGSQL_STMT_WHILE:
			dump_while((PLpgSQL_stmt_while *) stmt);
			break;
		case PLPGSQL_STMT_FORI:
			dump_fori((PLpgSQL_stmt_fori *) stmt);
			break;
		case PLPGSQL_STMT_FORS:
			dump_fors((PLpgSQL_stmt_fors *) stmt);
			break;
		case PLPGSQL_STMT_FORC:
			dump_forc((PLpgSQL_stmt_forc *) stmt);
			break;
		case PLPGSQL_STMT_FOREACH_A:
			dump_foreach_a((PLpgSQL_stmt_foreach_a *) stmt);
			break;
		case PLPGSQL_STMT_EXIT:
			dump_exit((PLpgSQL_stmt_exit *) stmt);
			break;
		case PLPGSQL_STMT_RETURN:
			dump_return((PLpgSQL_stmt_return *) stmt);
			break;
		case PLPGSQL_STMT_RETURN_NEXT:
			dump_return_next((PLpgSQL_stmt_return_next *) stmt);
			break;
		case PLPGSQL_STMT_RETURN_QUERY:
			dump_return_query((PLpgSQL_stmt_return_query *) stmt);
			break;
		case PLPGSQL_STMT_RAISE:
			dump_raise((PLpgSQL_stmt_raise *) stmt);
			break;
		case PLPGSQL_STMT_ASSERT:
			dump_assert((PLpgSQL_stmt_assert *) stmt);
			break;
		case PLPGSQL_STMT_EXECSQL:
			dump_execsql((PLpgSQL_stmt_execsql *) stmt);
			break;
		case PLPGSQL_STMT_DYNEXECUTE:
			dump_dynexecute((PLpgSQL_stmt_dynexecute *) stmt);
			break;
		case PLPGSQL_STMT_DYNFORS:
			dump_dynfors((PLpgSQL_stmt_dynfors *) stmt);
			break;
		case PLPGSQL_STMT_GETDIAG:
			dump_getdiag((PLpgSQL_stmt_getdiag *) stmt);
			break;
		case PLPGSQL_STMT_OPEN:
			dump_open((PLpgSQL_stmt_open *) stmt);
			break;
		case PLPGSQL_STMT_FETCH:
			dump_fetch((PLpgSQL_stmt_fetch *) stmt);
			break;
		case PLPGSQL_STMT_CLOSE:
			dump_close((PLpgSQL_stmt_close *) stmt);
			break;
		case PLPGSQL_STMT_PERFORM:
			dump_perform((PLpgSQL_stmt_perform *) stmt);
			break;
		case PLPGSQL_STMT_CALL:
			dump_call((PLpgSQL_stmt_call *) stmt);
			break;
		case PLPGSQL_STMT_COMMIT:
			dump_commit((PLpgSQL_stmt_commit *) stmt);
			break;
		case PLPGSQL_STMT_ROLLBACK:
			dump_rollback((PLpgSQL_stmt_rollback *) stmt);
			break;
		default:
			elog(ERROR, "unrecognized cmd_type: %d", stmt->cmd_type);
			break;
	}
}

static void
dump_stmts(List *stmts)
{
	ListCell   *s;

	dump_indent += 2;
	foreach(s, stmts)
		dump_stmt((PLpgSQL_stmt *) lfirst(s));
	dump_indent -= 2;
}

static void
dump_block(PLpgSQL_stmt_block *block)
{
	char	   *name;

	if (block->label == NULL)
		name = "*unnamed*";
	else
		name = block->label;

	dump_ind();
	printf("BLOCK <<%s>>\n", name);

	dump_stmts(block->body);

	if (block->exceptions)
	{
		ListCell   *e;

		foreach(e, block->exceptions->exc_list)
		{
			PLpgSQL_exception *exc = (PLpgSQL_exception *) lfirst(e);
			PLpgSQL_condition *cond;

			dump_ind();
			printf("    EXCEPTION WHEN ");
			for (cond = exc->conditions; cond; cond = cond->next)
			{
				if (cond != exc->conditions)
					printf(" OR ");
				printf("%s", cond->condname);
			}
			printf(" THEN\n");
			dump_stmts(exc->action);
		}
	}

	dump_ind();
	printf("    END -- %s\n", name);
}

static void
dump_assign(PLpgSQL_stmt_assign *stmt)
{
	dump_ind();
	printf("ASSIGN var %d := ", stmt->varno);
	dump_expr(stmt->expr);
	printf("\n");
}

static void
dump_if(PLpgSQL_stmt_if *stmt)
{
	ListCell   *l;

	dump_ind();
	printf("IF ");
	dump_expr(stmt->cond);
	printf(" THEN\n");
	dump_stmts(stmt->then_body);
	foreach(l, stmt->elsif_list)
	{
		PLpgSQL_if_elsif *elif = (PLpgSQL_if_elsif *) lfirst(l);

		dump_ind();
		printf("    ELSIF ");
		dump_expr(elif->cond);
		printf(" THEN\n");
		dump_stmts(elif->stmts);
	}
	if (stmt->else_body != NIL)
	{
		dump_ind();
		printf("    ELSE\n");
		dump_stmts(stmt->else_body);
	}
	dump_ind();
	printf("    ENDIF\n");
}

static void
dump_case(PLpgSQL_stmt_case *stmt)
{
	ListCell   *l;

	dump_ind();
	printf("CASE %d ", stmt->t_varno);
	if (stmt->t_expr)
		dump_expr(stmt->t_expr);
	printf("\n");
	dump_indent += 6;
	foreach(l, stmt->case_when_list)
	{
		PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);

		dump_ind();
		printf("WHEN ");
		dump_expr(cwt->expr);
		printf("\n");
		dump_ind();
		printf("THEN\n");
		dump_indent += 2;
		dump_stmts(cwt->stmts);
		dump_indent -= 2;
	}
	if (stmt->have_else)
	{
		dump_ind();
		printf("ELSE\n");
		dump_indent += 2;
		dump_stmts(stmt->else_stmts);
		dump_indent -= 2;
	}
	dump_indent -= 6;
	dump_ind();
	printf("    ENDCASE\n");
}

static void
dump_loop(PLpgSQL_stmt_loop *stmt)
{
	dump_ind();
	printf("LOOP\n");

	dump_stmts(stmt->body);

	dump_ind();
	printf("    ENDLOOP\n");
}

static void
dump_while(PLpgSQL_stmt_while *stmt)
{
	dump_ind();
	printf("WHILE ");
	dump_expr(stmt->cond);
	printf("\n");

	dump_stmts(stmt->body);

	dump_ind();
	printf("    ENDWHILE\n");
}

static void
dump_fori(PLpgSQL_stmt_fori *stmt)
{
	dump_ind();
	printf("FORI %s %s\n", stmt->var->refname, (stmt->reverse) ? "REVERSE" : "NORMAL");

	dump_indent += 2;
	dump_ind();
	printf("    lower = ");
	dump_expr(stmt->lower);
	printf("\n");
	dump_ind();
	printf("    upper = ");
	dump_expr(stmt->upper);
	printf("\n");
	if (stmt->step)
	{
		dump_ind();
		printf("    step = ");
		dump_expr(stmt->step);
		printf("\n");
	}
	dump_indent -= 2;

	dump_stmts(stmt->body);

	dump_ind();
	printf("    ENDFORI\n");
}

static void
dump_fors(PLpgSQL_stmt_fors *stmt)
{
	dump_ind();
	printf("FORS %s ", stmt->var->refname);
	dump_expr(stmt->query);
	printf("\n");

	dump_stmts(stmt->body);

	dump_ind();
	printf("    ENDFORS\n");
}

static void
dump_forc(PLpgSQL_stmt_forc *stmt)
{
	dump_ind();
	printf("FORC %s ", stmt->var->refname);
	printf("curvar=%d\n", stmt->curvar);

	dump_indent += 2;
	if (stmt->argquery != NULL)
	{
		dump_ind();
		printf("  arguments = ");
		dump_expr(stmt->argquery);
		printf("\n");
	}
	dump_indent -= 2;

	dump_stmts(stmt->body);

	dump_ind();
	printf("    ENDFORC\n");
}

static void
dump_foreach_a(PLpgSQL_stmt_foreach_a *stmt)
{
	dump_ind();
	printf("FOREACHA var %d ", stmt->varno);
	if (stmt->slice != 0)
		printf("SLICE %d ", stmt->slice);
	printf("IN ");
	dump_expr(stmt->expr);
	printf("\n");

	dump_stmts(stmt->body);

	dump_ind();
	printf("    ENDFOREACHA");
}

static void
dump_open(PLpgSQL_stmt_open *stmt)
{
	dump_ind();
	printf("OPEN curvar=%d\n", stmt->curvar);

	dump_indent += 2;
	if (stmt->argquery != NULL)
	{
		dump_ind();
		printf("  arguments = '");
		dump_expr(stmt->argquery);
		printf("'\n");
	}
	if (stmt->query != NULL)
	{
		dump_ind();
		printf("  query = '");
		dump_expr(stmt->query);
		printf("'\n");
	}
	if (stmt->dynquery != NULL)
	{
		dump_ind();
		printf("  execute = '");
		dump_expr(stmt->dynquery);
		printf("'\n");

		if (stmt->params != NIL)
		{
			ListCell   *lc;
			int			i;

			dump_indent += 2;
			dump_ind();
			printf("    USING\n");
			dump_indent += 2;
			i = 1;
			foreach(lc, stmt->params)
			{
				dump_ind();
				printf("    parameter $%d: ", i++);
				dump_expr((PLpgSQL_expr *) lfirst(lc));
				printf("\n");
			}
			dump_indent -= 4;
		}
	}
	dump_indent -= 2;
}

static void
dump_fetch(PLpgSQL_stmt_fetch *stmt)
{
	dump_ind();

	if (!stmt->is_move)
	{
		printf("FETCH curvar=%d\n", stmt->curvar);
		dump_cursor_direction(stmt);

		dump_indent += 2;
		if (stmt->target != NULL)
		{
			dump_ind();
			printf("    target = %d %s\n",
				   stmt->target->dno, stmt->target->refname);
		}
		dump_indent -= 2;
	}
	else
	{
		printf("MOVE curvar=%d\n", stmt->curvar);
		dump_cursor_direction(stmt);
	}
}

static void
dump_cursor_direction(PLpgSQL_stmt_fetch *stmt)
{
	dump_indent += 2;
	dump_ind();
	switch (stmt->direction)
	{
		case FETCH_FORWARD:
			printf("    FORWARD ");
			break;
		case FETCH_BACKWARD:
			printf("    BACKWARD ");
			break;
		case FETCH_ABSOLUTE:
			printf("    ABSOLUTE ");
			break;
		case FETCH_RELATIVE:
			printf("    RELATIVE ");
			break;
		default:
			printf("??? unknown cursor direction %d", stmt->direction);
	}

	if (stmt->expr)
	{
		dump_expr(stmt->expr);
		printf("\n");
	}
	else
		printf("%ld\n", stmt->how_many);

	dump_indent -= 2;
}

static void
dump_close(PLpgSQL_stmt_close *stmt)
{
	dump_ind();
	printf("CLOSE curvar=%d\n", stmt->curvar);
}

static void
dump_perform(PLpgSQL_stmt_perform *stmt)
{
	dump_ind();
	printf("PERFORM expr = ");
	dump_expr(stmt->expr);
	printf("\n");
}

static void
dump_call(PLpgSQL_stmt_call *stmt)
{
	dump_ind();
	printf("%s expr = ", stmt->is_call ? "CALL" : "DO");
	dump_expr(stmt->expr);
	printf("\n");
}

static void
dump_commit(PLpgSQL_stmt_commit *stmt)
{
	dump_ind();
	if (stmt->chain)
		printf("COMMIT AND CHAIN\n");
	else
		printf("COMMIT\n");
}

static void
dump_rollback(PLpgSQL_stmt_rollback *stmt)
{
	dump_ind();
	if (stmt->chain)
		printf("ROLLBACK AND CHAIN\n");
	else
		printf("ROLLBACK\n");
}

static void
dump_exit(PLpgSQL_stmt_exit *stmt)
{
	dump_ind();
	printf("%s", stmt->is_exit ? "EXIT" : "CONTINUE");
	if (stmt->label != NULL)
		printf(" label='%s'", stmt->label);
	if (stmt->cond != NULL)
	{
		printf(" WHEN ");
		dump_expr(stmt->cond);
	}
	printf("\n");
}

static void
dump_return(PLpgSQL_stmt_return *stmt)
{
	dump_ind();
	printf("RETURN ");
	if (stmt->retvarno >= 0)
		printf("variable %d", stmt->retvarno);
	else if (stmt->expr != NULL)
		dump_expr(stmt->expr);
	else
		printf("NULL");
	printf("\n");
}

static void
dump_return_next(PLpgSQL_stmt_return_next *stmt)
{
	dump_ind();
	printf("RETURN NEXT ");
	if (stmt->retvarno >= 0)
		printf("variable %d", stmt->retvarno);
	else if (stmt->expr != NULL)
		dump_expr(stmt->expr);
	else
		printf("NULL");
	printf("\n");
}

static void
dump_return_query(PLpgSQL_stmt_return_query *stmt)
{
	dump_ind();
	if (stmt->query)
	{
		printf("RETURN QUERY ");
		dump_expr(stmt->query);
		printf("\n");
	}
	else
	{
		printf("RETURN QUERY EXECUTE ");
		dump_expr(stmt->dynquery);
		printf("\n");
		if (stmt->params != NIL)
		{
			ListCell   *lc;
			int			i;

			dump_indent += 2;
			dump_ind();
			printf("    USING\n");
			dump_indent += 2;
			i = 1;
			foreach(lc, stmt->params)
			{
				dump_ind();
				printf("    parameter $%d: ", i++);
				dump_expr((PLpgSQL_expr *) lfirst(lc));
				printf("\n");
			}
			dump_indent -= 4;
		}
	}
}

static void
dump_raise(PLpgSQL_stmt_raise *stmt)
{
	ListCell   *lc;
	int			i = 0;

	dump_ind();
	printf("RAISE level=%d", stmt->elog_level);
	if (stmt->condname)
		printf(" condname='%s'", stmt->condname);
	if (stmt->message)
		printf(" message='%s'", stmt->message);
	printf("\n");
	dump_indent += 2;
	foreach(lc, stmt->params)
	{
		dump_ind();
		printf("    parameter %d: ", i++);
		dump_expr((PLpgSQL_expr *) lfirst(lc));
		printf("\n");
	}
	if (stmt->options)
	{
		dump_ind();
		printf("    USING\n");
		dump_indent += 2;
		foreach(lc, stmt->options)
		{
			PLpgSQL_raise_option *opt = (PLpgSQL_raise_option *) lfirst(lc);

			dump_ind();
			switch (opt->opt_type)
			{
				case PLPGSQL_RAISEOPTION_ERRCODE:
					printf("    ERRCODE = ");
					break;
				case PLPGSQL_RAISEOPTION_MESSAGE:
					printf("    MESSAGE = ");
					break;
				case PLPGSQL_RAISEOPTION_DETAIL:
					printf("    DETAIL = ");
					break;
				case PLPGSQL_RAISEOPTION_HINT:
					printf("    HINT = ");
					break;
				case PLPGSQL_RAISEOPTION_COLUMN:
					printf("    COLUMN = ");
					break;
				case PLPGSQL_RAISEOPTION_CONSTRAINT:
					printf("    CONSTRAINT = ");
					break;
				case PLPGSQL_RAISEOPTION_DATATYPE:
					printf("    DATATYPE = ");
					break;
				case PLPGSQL_RAISEOPTION_TABLE:
					printf("    TABLE = ");
					break;
				case PLPGSQL_RAISEOPTION_SCHEMA:
					printf("    SCHEMA = ");
					break;
			}
			dump_expr(opt->expr);
			printf("\n");
		}
		dump_indent -= 2;
	}
	dump_indent -= 2;
}

static void
dump_assert(PLpgSQL_stmt_assert *stmt)
{
	dump_ind();
	printf("ASSERT ");
	dump_expr(stmt->cond);
	printf("\n");

	dump_indent += 2;
	if (stmt->message != NULL)
	{
		dump_ind();
		printf("    MESSAGE = ");
		dump_expr(stmt->message);
		printf("\n");
	}
	dump_indent -= 2;
}

static void
dump_execsql(PLpgSQL_stmt_execsql *stmt)
{
	dump_ind();
	printf("EXECSQL ");
	dump_expr(stmt->sqlstmt);
	printf("\n");

	dump_indent += 2;
	if (stmt->target != NULL)
	{
		dump_ind();
		printf("    INTO%s target = %d %s\n",
			   stmt->strict ? " STRICT" : "",
			   stmt->target->dno, stmt->target->refname);
	}
	dump_indent -= 2;
}

static void
dump_dynexecute(PLpgSQL_stmt_dynexecute *stmt)
{
	dump_ind();
	printf("EXECUTE ");
	dump_expr(stmt->query);
	printf("\n");

	dump_indent += 2;
	if (stmt->target != NULL)
	{
		dump_ind();
		printf("    INTO%s target = %d %s\n",
			   stmt->strict ? " STRICT" : "",
			   stmt->target->dno, stmt->target->refname);
	}
	if (stmt->params != NIL)
	{
		ListCell   *lc;
		int			i;

		dump_ind();
		printf("    USING\n");
		dump_indent += 2;
		i = 1;
		foreach(lc, stmt->params)
		{
			dump_ind();
			printf("    parameter %d: ", i++);
			dump_expr((PLpgSQL_expr *) lfirst(lc));
			printf("\n");
		}
		dump_indent -= 2;
	}
	dump_indent -= 2;
}

static void
dump_dynfors(PLpgSQL_stmt_dynfors *stmt)
{
	dump_ind();
	printf("FORS %s EXECUTE ", stmt->var->refname);
	dump_expr(stmt->query);
	printf("\n");
	if (stmt->params != NIL)
	{
		ListCell   *lc;
		int			i;

		dump_indent += 2;
		dump_ind();
		printf("    USING\n");
		dump_indent += 2;
		i = 1;
		foreach(lc, stmt->params)
		{
			dump_ind();
			printf("    parameter $%d: ", i++);
			dump_expr((PLpgSQL_expr *) lfirst(lc));
			printf("\n");
		}
		dump_indent -= 4;
	}
	dump_stmts(stmt->body);
	dump_ind();
	printf("    ENDFORS\n");
}

static void
dump_getdiag(PLpgSQL_stmt_getdiag *stmt)
{
	ListCell   *lc;

	dump_ind();
	printf("GET %s DIAGNOSTICS ", stmt->is_stacked ? "STACKED" : "CURRENT");
	foreach(lc, stmt->diag_items)
	{
		PLpgSQL_diag_item *diag_item = (PLpgSQL_diag_item *) lfirst(lc);

		if (lc != list_head(stmt->diag_items))
			printf(", ");

		printf("{var %d} = %s", diag_item->target,
			   plpgsql_getdiag_kindname(diag_item->kind));
	}
	printf("\n");
}

static void
dump_expr(PLpgSQL_expr *expr)
{
	printf("'%s'", expr->query);
	if (expr->target_param >= 0)
		printf(" target %d%s", expr->target_param,
			   expr->target_is_local ? " (local)" : "");
}

void
plpgsql_dumptree(PLpgSQL_function *func)
{
	int			i;
	PLpgSQL_datum *d;

	printf("\nExecution tree of successfully compiled PL/pgSQL function %s:\n",
		   func->fn_signature);

	printf("\nFunction's data area:\n");
	for (i = 0; i < func->ndatums; i++)
	{
		d = func->datums[i];

		printf("    entry %d: ", i);
		switch (d->dtype)
		{
			case PLPGSQL_DTYPE_VAR:
			case PLPGSQL_DTYPE_PROMISE:
				{
					PLpgSQL_var *var = (PLpgSQL_var *) d;

					printf("VAR %-16s type %s (typoid %u) atttypmod %d\n",
						   var->refname, var->datatype->typname,
						   var->datatype->typoid,
						   var->datatype->atttypmod);
					if (var->isconst)
						printf("                                  CONSTANT\n");
					if (var->notnull)
						printf("                                  NOT NULL\n");
					if (var->default_val != NULL)
					{
						printf("                                  DEFAULT ");
						dump_expr(var->default_val);
						printf("\n");
					}
					if (var->cursor_explicit_expr != NULL)
					{
						if (var->cursor_explicit_argrow >= 0)
							printf("                                  CURSOR argument row %d\n", var->cursor_explicit_argrow);

						printf("                                  CURSOR IS ");
						dump_expr(var->cursor_explicit_expr);
						printf("\n");
					}
					if (var->promise != PLPGSQL_PROMISE_NONE)
						printf("                                  PROMISE %d\n",
							   (int) var->promise);
				}
				break;
			case PLPGSQL_DTYPE_ROW:
				{
					PLpgSQL_row *row = (PLpgSQL_row *) d;

					printf("ROW %-16s fields", row->refname);
					for (int j = 0; j < row->nfields; j++)
					{
						printf(" %s=var %d", row->fieldnames[j],
							   row->varnos[j]);
					}
					printf("\n");
				}
				break;
			case PLPGSQL_DTYPE_REC:
				printf("REC %-16s typoid %u\n",
					   ((PLpgSQL_rec *) d)->refname,
					   ((PLpgSQL_rec *) d)->rectypeid);
				if (((PLpgSQL_rec *) d)->isconst)
					printf("                                  CONSTANT\n");
				if (((PLpgSQL_rec *) d)->notnull)
					printf("                                  NOT NULL\n");
				if (((PLpgSQL_rec *) d)->default_val != NULL)
				{
					printf("                                  DEFAULT ");
					dump_expr(((PLpgSQL_rec *) d)->default_val);
					printf("\n");
				}
				break;
			case PLPGSQL_DTYPE_RECFIELD:
				printf("RECFIELD %-16s of REC %d\n",
					   ((PLpgSQL_recfield *) d)->fieldname,
					   ((PLpgSQL_recfield *) d)->recparentno);
				break;
			default:
				printf("??? unknown data type %d\n", d->dtype);
		}
	}
	printf("\nFunction's statements:\n");

	dump_indent = 0;
	printf("%3d:", func->action->lineno);
	dump_block(func->action);
	printf("\nEnd of execution tree of function %s\n\n", func->fn_signature);
	fflush(stdout);
}
