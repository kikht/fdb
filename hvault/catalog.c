#include "catalog.h"
#include "deparse.h"

struct HvaultCatalogQueryData
{
    HvaultDeparseContext deparse;
    StringInfoData products;
    char const * sort_qual;
    size_t limit_qual;
};

#define NO_LIMIT ((size_t)(-1))

/*
 * Query construction routines
 */

/* Creates new catalog query builder and initialize it*/
HvaultCatalogQuery 
hvaultCatalogInitQuery (HvaultTableInfo const * table)
{
    HvaultCatalogQuery query = palloc(sizeof(struct HvaultCatalogQueryData));
    initStringInfo(&query->products);
    hvaultDeparseContextInit(&query->deparse, table);
    query->sort_qual = NULL;
    query->limit_qual = NO_LIMIT;
    return query;
}

/* Creates deep copy of a query */
HvaultCatalogQuery 
hvaultCatalogCloneQuery (HvaultCatalogQuery query)
{
    HvaultCatalogQuery res = hvaultCatalogInitQuery(query->deparse.table);
    res->deparse.fdw_expr = list_copy(query->deparse.fdw_expr);
    appendStringInfoString(&res->deparse.query, query->deparse.query.data);
    appendStringInfoString(&res->products, query->products.data);
    if (query->sort_qual != NULL)
        res->sort_qual = pstrdup(query->sort_qual);
    res->limit_qual = query->limit_qual;
    return res;
}

/* Frees builder and all it's data */
void 
hvaultCatalogFreeQuery (HvaultCatalogQuery query)
{
    hvaultDeparseContextFree(&query->deparse);
    if (query->sort_qual != NULL)
        pfree((void *) query->sort_qual);

    pfree(query->products.data);
}

/* Add required product to query */
void 
hvaultCatalogAddProduct (HvaultCatalogQuery query, char const * name)
{
    if (query->products.len != 0) 
    {
        appendStringInfoString(&query->products, ", ");
    }
    appendStringInfoString(&query->products, name);
}

/* Add catalog qual to query */
void 
hvaultCatalogAddQual (HvaultCatalogQuery query,
                      HvaultQual *       qual)
{
    appendStringInfoString(&query->deparse.query, 
        query->deparse.query.len == 0 ? " WHERE " : " AND ");
    hvaultDeparseQual(qual, &query->deparse);
}

/* Add sort qual to query */
void 
hvaultCatalogSetSort (HvaultCatalogQuery query, 
                      char const *       qual)
{
    query->sort_qual = qual != NULL ? pstrdup(qual) : NULL;
}

/* Add limit qual to query */
void 
hvaultCatalogSetLimit (HvaultCatalogQuery query, 
                       size_t             limit)
{
    query->limit_qual = limit;
}

/* Get list of required parameter expressions that need to be evaluated 
   and passed to query cursor */
List * 
hvaultCatalogGetParams (HvaultCatalogQuery query)
{
    return list_copy(query->deparse.fdw_expr);
}

static void buildQueryString (HvaultCatalogQuery query, StringInfo query_str)
{
    appendStringInfoString(query_str, "SELECT file_id, starttime, stoptime, ");
    Assert(query->products.len != 0);
    appendStringInfoString(query_str, query->products.data);
    appendStringInfoString(query_str, " FROM ");
    appendStringInfoString(query_str, 
                           quote_identifier(query->deparse.table->catalog));
    appendStringInfoString(query_str, query->deparse.query.data);

    if (query->sort_qual)
    {
        appendStringInfoString(query_str, " ORDER BY ");
        appendStringInfoString(query_str, query->sort_qual);
    }

    if (query->limit_qual != NO_LIMIT)
    {
        appendStringInfo(query_str, " LIMIT %lu", query->limit_qual);
    }
}

/* Get cost estimate of catalog query */
void 
hvaultCatalogGetCosts (HvaultCatalogQuery query,
                       Cost *             startup_cost,
                       Cost *             total_cost,
                       double *           plan_rows,
                       int *              plan_width)
{
    MemoryContext oldmemctx, memctx;
    List *parsetree, *stmt_list;
    PlannedStmt *plan;
    StringInfoData query_str;
    int nargs, argno;
    Oid *argtypes;
    ListCell *l;

    memctx = AllocSetContextCreate(CurrentMemoryContext, 
                                   "hvaultQueryCosts context", 
                                   ALLOCSET_DEFAULT_MINSIZE,
                                   ALLOCSET_DEFAULT_INITSIZE,
                                   ALLOCSET_DEFAULT_MAXSIZE);
    oldmemctx = MemoryContextSwitchTo(memctx);

    initStringInfo(&query_str);
    buildQueryString(query, &query_str);

    nargs = list_length(query->deparse.fdw_expr);
    argtypes = palloc(sizeof(Oid) * nargs);
    argno = 0;
    foreach(l, query->deparse.fdw_expr)
    {
        Node *expr = lfirst(l);
        argtypes[argno] = exprType(expr);
        argno++;
    }

    parsetree = pg_parse_query(query_str.data);
    Assert(list_length(parsetree) == 1);
    stmt_list = pg_analyze_and_rewrite(linitial(parsetree), 
                                       query_str.data, 
                                       argtypes, 
                                       nargs);
    Assert(list_length(stmt_list) == 1);
    plan = pg_plan_query((Query *) linitial(stmt_list), 
                         CURSOR_OPT_GENERIC_PLAN, 
                         NULL);

    *startup_cost = plan->planTree->startup_cost;
    *total_cost = plan->planTree->total_cost;
    *plan_rows = plan->planTree->plan_rows;
    *plan_width = plan->planTree->plan_width;

    MemoryContextSwitchTo(oldmemctx);
    MemoryContextDelete(memctx);
}

/* Pack query to form that postgres knows how to copy */
List * 
hvaultCatalogPackQuery(HvaultCatalogQuery query)
{
    StringInfoData query_str;
    initStringInfo(&query_str);
    buildQueryString(query, &query_str);
    return list_make2(makeString(query_str.data),
                      makeInteger(list_length(query->deparse.fdw_expr)));
}

/*
 * Catalog cursor routines
 */

struct HvaultCatalogCursorData 
{
    char const *  query;         /* Catalog query string */
    SPIPlanPtr    prep_stmt;
    int           nargs;
    char const *  name;
    MemoryContext memctx;
    TupleDesc     tupdesc;
    HeapTuple     tuple;
};

/* Creates new catalog cursor and initializes it with packed query */
HvaultCatalogCursor 
hvaultCatalogInitCursor (List * packed_query, MemoryContext memctx)
{
    struct HvaultCatalogCursorData * cursor = NULL;

    cursor = palloc(sizeof(struct HvaultCatalogCursorData));
    Assert(list_length(packed_query) == 2);
    cursor->query = strVal(linitial(packed_query));
    cursor->nargs = intVal(lsecond(packed_query));
    cursor->prep_stmt = NULL;
    cursor->name = NULL;
    cursor->tupdesc = NULL;
    cursor->tuple = NULL;
    cursor->memctx = AllocSetContextCreate(memctx,
                                           "hvault_fdw catalog cursor data",
                                           ALLOCSET_SMALL_MINSIZE,
                                           ALLOCSET_SMALL_INITSIZE,
                                           ALLOCSET_SMALL_MAXSIZE);
    return cursor;
}

/* Destroys cursor and all it's data */
void 
hvaultCatalogFreeCursor (HvaultCatalogCursor cursor)
{
    if (cursor->tupdesc != NULL)
    {
        FreeTupleDesc(cursor->tupdesc);
        cursor->tupdesc = NULL;
    }
    if (cursor->tuple) 
    {
        heap_freetuple(cursor->tuple);
        cursor->tuple = NULL;
    }
    if (cursor->memctx != NULL) 
    {
        MemoryContextDelete(cursor->memctx);
        cursor->memctx = NULL;    
    }
    
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't connect to SPI")));
        return; /* Will never reach this */
    }


    if (cursor->name != NULL)
    {
        Portal file_cursor = SPI_cursor_find(cursor->name);        
        SPI_cursor_close(file_cursor);
        cursor->name = NULL;
    }


    if (cursor->prep_stmt != NULL)
    {
        SPI_freeplan(cursor->prep_stmt);
        cursor->prep_stmt = NULL;
    }

    if (SPI_finish() != SPI_OK_FINISH)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't finish access to SPI")));
        return; /* Will never reach this */
    }
    
    pfree(cursor);
}

int 
hvaultCatalogGetNumArgs (HvaultCatalogCursor cursor)
{
    return cursor->nargs;
}

static HvaultCatalogCursorResult 
fetchTuple (HvaultCatalogCursor cursor, Portal file_cursor)
{
    MemoryContext oldmemctx = NULL;

    Assert(file_cursor);
    SPI_cursor_fetch(file_cursor, true, 1);
    if (SPI_processed != 1 || SPI_tuptable == NULL)
    {
        /* Can't fetch more files */
        elog(DEBUG1, "No more files");
        return HvaultCatalogCursorEOF;
    }   
    
    if (cursor->tupdesc == NULL) 
    {
        int i;

        if (SPI_tuptable->tupdesc->natts < 4 ||
            SPI_tuptable->tupdesc->attrs[0]->atttypid != INT4OID ||
            SPI_tuptable->tupdesc->attrs[1]->atttypid != TIMESTAMPOID ||
            SPI_tuptable->tupdesc->attrs[2]->atttypid != TIMESTAMPOID)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Error in catalog query (header)")));
            return HvaultCatalogCursorError; /* Will never reach this */         
        }
        for (i = 3; i < SPI_tuptable->tupdesc->natts; i++)
        {
            if (SPI_tuptable->tupdesc->attrs[i]->atttypid != TEXTOID)
            {
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Error in catalog query (products)")));
                return HvaultCatalogCursorError; /* Will never reach this */                
            }
        }

        oldmemctx = MemoryContextSwitchTo(cursor->memctx);
        cursor->tupdesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);
        MemoryContextSwitchTo(oldmemctx);
    }

    oldmemctx = MemoryContextSwitchTo(cursor->memctx);
    if (cursor->tuple)
        heap_freetuple(cursor->tuple);
    cursor->tuple = heap_copytuple(SPI_tuptable->vals[0]);
    MemoryContextSwitchTo(oldmemctx);

    return HvaultCatalogCursorOK;
}

/* Starts cursor with specified parameters */
void 
hvaultCatalogStartCursor (HvaultCatalogCursor cursor, 
                          Oid *               argtypes,
                          Datum *             argvals,
                          char *              argnulls)
{
    Portal file_cursor;

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't connect to SPI")));
        return; /* Will never reach this */
    }

    if (cursor->prep_stmt == NULL)
    {
        cursor->prep_stmt = SPI_prepare(cursor->query, cursor->nargs, argtypes);
        if (!cursor->prep_stmt)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't prepare query for catalog: %s", 
                                   cursor->query)));
            return; /* Will never reach this */
        }
        if (SPI_keepplan(cursor->prep_stmt) != 0)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't save prepared plan")));
            return; /* Will never reach this */
        }
    }

    file_cursor = SPI_cursor_open(NULL, cursor->prep_stmt, 
                                  argvals, argnulls, true);
    cursor->name = file_cursor->name;

    if (SPI_finish() != SPI_OK_FINISH)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't finish access to SPI")));
        return; /* Will never reach this */
    }
}

/* Resets cursor. Next invocation of hvaultCatalogNext will return 
   HvaultCatalogCursorNotStarted */
void 
hvaultCatlogResetCursor (HvaultCatalogCursor cursor)
{
    if (cursor->tuple) 
    {
        heap_freetuple(cursor->tuple);
        cursor->tuple = NULL;
    }

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't connect to SPI")));
        return; /* Will never reach this */
    }

    if (cursor->name != NULL)
    {
        Portal file_cursor = SPI_cursor_find(cursor->name);        
        SPI_cursor_close(file_cursor);
        cursor->name = NULL;
    }

    if (SPI_finish() != SPI_OK_FINISH)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't finish access to SPI")));
        return; /* Will never reach this */
    }
}

/* Moves cursor to the next catalog record */
HvaultCatalogCursorResult 
hvaultCatalogNext (HvaultCatalogCursor cursor)
{
    Portal file_cursor;
    HvaultCatalogCursorResult res;

    if (cursor->name == NULL)
    {
        return HvaultCatalogCursorNotStarted;
    }

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't connect to SPI")));
        return HvaultCatalogCursorError; /* Will never reach this */
    }    

    file_cursor = SPI_cursor_find(cursor->name);
    res = fetchTuple(cursor, file_cursor);
 
    if (SPI_finish() != SPI_OK_FINISH)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't finish access to SPI")));
        return HvaultCatalogCursorError; /* Will never reach this */
    }   

    return res;
}

/* Gets current record's id */
int 
hvaultCatalogGetId (HvaultCatalogCursor cursor)
{
    Datum val;
    bool isnull;
    val = heap_getattr(cursor->tuple, 1, cursor->tupdesc, &isnull);
    if (!isnull)
    {
        return DatumGetInt32(val);
    }
    else
    {
        elog(WARNING, "Wrong entry in catalog, null file_id");
        return 0;
    }
}

/* Get current records's start time */
Timestamp 
hvaultCatalogGetStarttime (HvaultCatalogCursor cursor)
{
    Datum val;
    bool isnull;
    val = heap_getattr(cursor->tuple, 2, cursor->tupdesc, &isnull);
    return isnull ? 0 : DatumGetTimestamp(val);
}

/* Get current records's stop time */
Timestamp 
hvaultCatalogGetStoptime (HvaultCatalogCursor cursor)
{
    Datum val;
    bool isnull;
    val = heap_getattr(cursor->tuple, 3, cursor->tupdesc, &isnull);
    return isnull ? 0 : DatumGetTimestamp(val);   
}

/* Get current record's product filename */
char const * 
hvaultCatalogGetFilename (HvaultCatalogCursor cursor,
                          char const *        product)
{
    int colno;
    colno = SPI_fnumber(cursor->tupdesc, product);
    if (colno == SPI_ERROR_NOATTRIBUTE) 
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't find specified product in query")));
    }
    return SPI_getvalue(cursor->tuple, cursor->tupdesc, colno);
}

double 
hvaultGetNumFiles(char const *catalog)
{
    StringInfo query_str;
    Datum val;
    int64_t num_files;
    bool isnull;

    if (!catalog)
    {
        /* Single file mode */
        return 1;
    }

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                     errmsg("Can't connect to SPI")));
        return 0; /* Will never reach this */
    }

    query_str = makeStringInfo();
    /* Add constraints */
    appendStringInfo(query_str, "SELECT COUNT(*) FROM %s", catalog);
    if (SPI_execute(query_str->data, true, 1) != SPI_OK_SELECT || 
        SPI_processed != 1)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't get number of rows in catalog %s", 
                               catalog)));
        return 0; /* Will never reach this */      
    }
    pfree(query_str->data);
    pfree(query_str);
    if (SPI_tuptable->tupdesc->natts != 1 ||
        SPI_tuptable->tupdesc->attrs[0]->atttypid != INT8OID)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't get number of rows in catalog %s", 
                               catalog)));
        return 0; /* Will never reach this */         
    }
    val = heap_getattr(SPI_tuptable->vals[0], 1, 
                       SPI_tuptable->tupdesc, &isnull);
    if (isnull)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't get number of rows in catalog %s", 
                               catalog)));
        return 0; /* Will never reach this */            
    }
    num_files = DatumGetInt64(val);
    if (SPI_finish() != SPI_OK_FINISH)    
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't finish access to SPI")));
        return 0; /* Will never reach this */   
    }
    return num_files;
}

char const * 
hvaultCatalogGetQuery (HvaultCatalogCursor cursor)
{
    return cursor->query;
}
