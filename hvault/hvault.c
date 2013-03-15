#include "hvault.h"

/* PostgreSQL */
#include <access/skey.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <commands/explain.h>
#include <commands/vacuum.h>
#include <executor/spi.h>
#include <foreign/fdwapi.h>
#include <foreign/foreign.h>
#include <nodes/bitmapset.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/planmain.h>
#include <optimizer/restrictinfo.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/rel.h>
#include <utils/timestamp.h>

/* HDF */
#include "hdf.h"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* 
 * FDW callback routines 
 */
static void hvaultExplain(ForeignScanState *node, ExplainState *es);
static void hvaultBegin(ForeignScanState *node, int eflags);
static TupleTableSlot *hvaultIterate(ForeignScanState *node);
static void hvaultReScan(ForeignScanState *node);
static void hvaultEnd(ForeignScanState *node);
static bool hvaultAnalyze(Relation relation, 
                          AcquireSampleRowsFunc *func,
                          BlockNumber *totalpages);

/* Initialization functions */
static HvaultExecState *makeExecState(List *coltypes, 
                                      MemoryContext memctx, 
                                      List *fdw_expr, 
                                      PlanState *plan,
                                      List *predicates);
static HvaultCatalogCursor *makeCatalogCursor(char const *query, 
                                              List *fdw_expr);
static void assignSDSBuffers(HvaultExecState *state, 
                             Oid relid, 
                             TupleDesc tupdesc);
static void  check_column_types(List *coltypes, TupleDesc tupdesc);

/* Iteration functions */
static void startCatalogCursor(HvaultCatalogCursor *cursor, 
                               List *fdw_expr, 
                               ExprContext *expr_ctx);
static bool fetch_next_file(HvaultExecState *scan);
static void fetch_next_line(HvaultExecState *scan);
static void calc_next_footprint(HvaultExecState const *scan, 
                                HvaultHDFFile *file);
/* 
 * Tuple fill utilities
 */
static inline void fill_tuple(HvaultExecState const *scan);

static int acquire_sample_rows(Relation relation,
                               int elevel,
                               HeapTuple *rows,
                               int targrows,
                               double *totalrows,
                               double *totaldeadrows);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
PG_FUNCTION_INFO_V1(hvault_fdw_handler);
Datum 
hvault_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    fdwroutine->GetForeignRelSize   = hvaultGetRelSize;
    fdwroutine->GetForeignPaths     = hvaultGetPaths;
    fdwroutine->GetForeignPlan      = hvaultGetPlan;
    fdwroutine->ExplainForeignScan  = hvaultExplain;
    fdwroutine->BeginForeignScan    = hvaultBegin;
    fdwroutine->IterateForeignScan  = hvaultIterate;
    fdwroutine->ReScanForeignScan   = hvaultReScan;
    fdwroutine->EndForeignScan      = hvaultEnd;
    fdwroutine->AnalyzeForeignTable = hvaultAnalyze;

    PG_RETURN_POINTER(fdwroutine);
}

PG_FUNCTION_INFO_V1(hvault_fdw_validator);
Datum 
hvault_fdw_validator(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(true);
}

static void 
hvaultExplain(ForeignScanState *node, ExplainState *es)
{
    /* Print additional EXPLAIN output for a foreign table scan. This can just
       return if there is no need to print anything. Otherwise, it should call
       ExplainPropertyText and related functions to add fields to the EXPLAIN
       output. The flag fields in es can be used to determine what to print, and
       the state of the ForeignScanState node can be inspected to provide run-
       time statistics in the EXPLAIN ANALYZE case. */    

    ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
    List *fdw_plan_private = plan->fdw_private;
    Oid foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
    char *catalog = hvaultGetTableOption(foreigntableid, "catalog");
    if (catalog)
    {
        Value *query = list_nth(fdw_plan_private, HvaultPlanCatalogQuery);
        ExplainPropertyText("Catalog query", strVal(query), es);
    }
    else
    {
        char *filename = hvaultGetTableOption(foreigntableid, "filename");
        if (filename != NULL)
            ExplainPropertyText("Single file", filename, es);
    }
}

static void 
hvaultBegin(ForeignScanState *node, int eflags)
{
    HvaultExecState *state;
    Oid foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
    ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
    List *fdw_plan_private = plan->fdw_private;
    TupleDesc tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
    char *catalog;


    elog(DEBUG1, "in hvaultBegin");
    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    Assert(list_length(fdw_plan_private) == HvaultPlanNumParams);
    state = makeExecState(list_nth(fdw_plan_private, HvaultPlanColtypes), 
                          node->ss.ps.state->es_query_cxt,
                          plan->fdw_exprs,
                          &node->ss.ps,
                          list_nth(fdw_plan_private, HvaultPlanPredicates));
    check_column_types(state->coltypes, tupdesc);

    catalog = hvaultGetTableOption(foreigntableid, "catalog");
    if (catalog)
    {
        Value *query = list_nth(fdw_plan_private, HvaultPlanCatalogQuery);
        state->cursor = makeCatalogCursor(strVal(query), plan->fdw_exprs);
    }
    else
    {
        state->file.filename = hvaultGetTableOption(foreigntableid, 
                                                    "filename");
        if (!state->file.filename)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't find catalog or filename option")));
        }
    }

    /* fill required sds list & helper pointers */
    assignSDSBuffers(state, foreigntableid, tupdesc);
    elog(DEBUG1, "SDS buffers: %d", list_length(state->file.sds));

    node->fdw_state = state;
}

static TupleTableSlot *
hvaultIterate(ForeignScanState *node) 
{
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    /*TupleDesc tupdesc = slot->tts_tupleDescriptor;*/
    HvaultExecState *fdw_exec_state = (HvaultExecState *) node->fdw_state;

    ExecClearTuple(slot);

    /* Next line needed? (initial state -1 == -1) */
    while (fdw_exec_state->cur_sel == fdw_exec_state->file.sel_size)
    {
        ListCell *l;
        size_t *src_sel;

        
        fdw_exec_state->cur_sel = 0;
        fdw_exec_state->cur_sample = -1;
        fdw_exec_state->cur_line++;
        /* Next file needed? */
        if (fdw_exec_state->cur_line >= fdw_exec_state->file.num_lines) {
            if (fdw_exec_state->cursor)
            {
                if (!fetch_next_file(fdw_exec_state))
                {
                    /* End of scan, return empty tuple*/
                    elog(DEBUG1, "End of scan: no more files");
                    return slot;
                }
                fdw_exec_state->cur_line = 0;
            } 
            else 
            {
                /* Single file case */
                /* Initialization or end of file? */
                if (fdw_exec_state->file.sd_id == FAIL)
                {
                    char const *filename = fdw_exec_state->file.filename;
                    if (!hdf_file_open(&fdw_exec_state->file, filename, 
                                       fdw_exec_state->has_footprint))
                    {
                        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                                        errmsg("Can't open file %s", 
                                               filename)));
                        return NULL; /* Will never reach here */
                    }
                    fdw_exec_state->cur_file = 0;
                }
                else
                {
                    /* End of scan, returning empty tuple */
                    return slot;
                }
            }
        }
        fetch_next_line(fdw_exec_state);

        /* Predicate calculation */
        src_sel = NULL;
        fdw_exec_state->file.sel_size = fdw_exec_state->file.num_samples;
        foreach(l, fdw_exec_state->predicates)
        {
            HvaultGeomPredicate *p = (HvaultGeomPredicate *) lfirst(l);
            ExprState *expr = list_nth(fdw_exec_state->fdw_expr, p->argno);
            bool isnull;
            Datum argdatum = ExecEvalExpr(expr, fdw_exec_state->expr_ctx, 
                                          &isnull, NULL);
            GSERIALIZED *arggeom = (GSERIALIZED *) DatumGetPointer(argdatum);
            GBOX arg;
            if (gserialized_get_gbox_p(arggeom, &arg) == LW_FAILURE)
            {
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                                errmsg("Can't get GBOX from predicate arg")));
                return NULL; /* Will never reach here */
            }

            fdw_exec_state->file.sel_size = 
                    hvaultGeomPredicate(p->coltype, 
                                        p->op, 
                                        p->isneg, 
                                        fdw_exec_state, 
                                        &arg, 
                                        fdw_exec_state->file.sel_size, 
                                        src_sel,
                                        fdw_exec_state->file.sel);
            src_sel = fdw_exec_state->file.sel;
            if (fdw_exec_state->file.sel_size <= 0)
            {
                break;
            }

        }
    }

    if (fdw_exec_state->file.sel_size < fdw_exec_state->file.num_samples)
    {
        fdw_exec_state->cur_sample = 
            fdw_exec_state->file.sel[fdw_exec_state->cur_sel];
    }
    else
    {
        fdw_exec_state->cur_sample = fdw_exec_state->cur_sel;
    }
    fdw_exec_state->cur_sel++;
    
    /* Fill tuple */
    fill_tuple(fdw_exec_state);
    slot->tts_isnull = fdw_exec_state->nulls;
    slot->tts_values = fdw_exec_state->values;
    ExecStoreVirtualTuple(slot);
    return slot;
}

static void 
hvaultReScan(ForeignScanState *node)
{
    HvaultExecState *state = (HvaultExecState *) node->fdw_state;
    elog(DEBUG1, "in hvaultReScan");
    if (state == NULL)
        return;

    if (state->file.sd_id != FAIL)
    {
        hdf_file_close(&state->file);
    }

    if (state->cursor)
    {
        if (SPI_connect() != SPI_OK_CONNECT)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't connect to SPI")));
            return; /* Will never reach this */
        }
    
        if (state->cursor->file_cursor_name != NULL)
        {
            Portal file_cursor = 
                SPI_cursor_find(state->cursor->file_cursor_name);
            SPI_cursor_close(file_cursor);

        }

        if (SPI_finish() != SPI_OK_FINISH)    
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't finish access to SPI")));
            return; /* Will never reach this */   
        }

        state->cursor->file_cursor_name = NULL;
    }

    MemoryContextReset(state->file.filememcxt);

    state->cur_file = -1;
    state->cur_line = -1;
    state->cur_sample = -1;
    state->cur_sel = -1;
}


static void 
hvaultEnd(ForeignScanState *node)
{
    HvaultExecState *state = (HvaultExecState *) node->fdw_state;
    elog(DEBUG1, "in hvaultEnd");
    if (state == NULL)
        return;
    
    if (state->file.sd_id != FAIL)
    {
        hdf_file_close(&state->file);
    }

    if (state->cursor)
    {
        if (SPI_connect() != SPI_OK_CONNECT)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't connect to SPI")));
            return; /* Will never reach this */
        }
    
        if (state->cursor->file_cursor_name != NULL)
        {
            Portal file_cursor = 
                SPI_cursor_find(state->cursor->file_cursor_name);
            SPI_cursor_close(file_cursor);

        }
        if (state->cursor->prep_stmt != NULL)
        {
            SPI_freeplan(state->cursor->prep_stmt);
        }
        
        if (SPI_finish() != SPI_OK_FINISH)    
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't finish access to SPI")));
            return; /* Will never reach this */   
        }
    }
    
    MemoryContextDelete(state->file.filememcxt);
}

static bool 
hvaultAnalyze(Relation relation, 
              AcquireSampleRowsFunc *func,
              BlockNumber *totalpages)
{
    char *catalog;
    StringInfo query;
    Datum val;
    bool isnull;
    Oid argtypes[] = {INT4OID};
    Datum argvals[] = {BLCKSZ};


    catalog = hvaultGetTableOption(RelationGetRelid(relation), "catalog");
    if (catalog == NULL)
        return false;

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't connect to SPI")));
        return false; /* Will never reach this */
    }

    query = makeStringInfo();
    appendStringInfo(query, 
                     "SELECT CAST(((SUM(size) + $1 - 1)/$1) AS float8) FROM %s", 
                     catalog);
    if (SPI_execute_with_args(query->data, 1, argtypes, argvals, NULL, true, 1) 
            != SPI_OK_SELECT ||
        SPI_processed != 1 ||
        SPI_tuptable->tupdesc->natts != 1 ||
        SPI_tuptable->tupdesc->attrs[0]->atttypid != FLOAT8OID)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't select from catalog %s", catalog)));
        return false; /* Will never reach this */
    }
    pfree(query->data);
    pfree(query);

    val = heap_getattr(SPI_tuptable->vals[0], 1, 
                       SPI_tuptable->tupdesc, &isnull);
    if (isnull)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't select from catalog %s", catalog)));
        return false; /* Will never reach this */            
    }


    *totalpages = DatumGetFloat8(val);
    if (*totalpages < 1)
        *totalpages = 1;

    if (SPI_finish() != SPI_OK_FINISH)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't finish access to SPI")));
        return false; /* Will never reach this */
    }
    *func = acquire_sample_rows;
    return true;
}




static int
acquire_sample_rows(Relation relation,
                    int elevel,
                    HeapTuple *rows,
                    int targrows,
                    double *totalrows,
                    double *totaldeadrows)
{
    char *catalog;
    HvaultExecState *state;
    StringInfoData query;
    List *coltypes;
    Oid foreigntableid;
    TupleDesc tupdesc;
    int num_rows = 0;
    int num_files;
    double rowstoskip = -1;
    double rstate = anl_init_selection_state(targrows);
    double procrows = 0;


    foreigntableid = RelationGetRelid(relation);
    tupdesc = RelationGetDescr(relation);
    catalog = hvaultGetTableOption(foreigntableid, "catalog");
    if (catalog == NULL)
        return 0;

    coltypes = hvaultGetAllColumns(relation);
    state = makeExecState(coltypes, CurrentMemoryContext, NIL, NULL, NIL);

    num_files = targrows / 100;
    if (num_files < 5)
        num_files = 5;

    initStringInfo(&query);
    appendStringInfo(&query, 
                     "%s %s ORDER BY random() LIMIT %d", 
                     HVAULT_CATALOG_QUERY_PREFIX,
                     catalog,
                     num_files);
    state->cursor = makeCatalogCursor(query.data, NIL);
    
    check_column_types(state->coltypes, tupdesc);
    assignSDSBuffers(state, foreigntableid, tupdesc);

    while (fetch_next_file(state))
    {
        for (state->cur_line = 0; state->cur_line < state->file.num_lines;
             state->cur_line++)
        {
            vacuum_delay_point();

            fetch_next_line(state);
            for (state->cur_sample = 0; 
                 state->cur_sample < state->file.num_samples;
                 state->cur_sample++)
            {
                int pos = -1;
                procrows += 1;
                if (num_rows >= targrows)
                {
                    if (rowstoskip < 0)
                        rowstoskip = anl_get_next_S(procrows, targrows, 
                                                    &rstate);

                    if (rowstoskip <= 0)
                    {
                        pos = (int) (anl_random_fract() * targrows);
                        heap_freetuple(rows[pos]);
                    }
                    else
                    {
                        pos = -1;
                    }
                    rowstoskip -= 1;
                }
                else
                {
                    pos = num_rows;
                    num_rows++;
                }

                if (pos >= 0)
                {
                    fill_tuple(state);
                    rows[pos] = heap_form_tuple(tupdesc, state->values, 
                                                state->nulls);
                }
            }
        }
    }

    *totalrows = hvaultGetNumFiles(catalog) * HVAULT_TUPLES_PER_FILE;
    *totaldeadrows = 0;
    return num_rows;
}

static void
check_column_types(List *coltypes, TupleDesc tupdesc)
{
    ListCell *l;
    AttrNumber attnum = 0;
    Oid geomtypeoid = TypenameGetTypid("geometry");
    if (geomtypeoid == InvalidOid) 
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't find geometry type"),
                        errhint("PostGIS must be installed to use Hvault")));
    }

    elog(DEBUG1, "check_column_types: coltypes: %s", nodeToString(coltypes));

    Assert(list_length(coltypes) == tupdesc->natts);
    foreach(l, coltypes)
    {
        switch(lfirst_int(l))
        {
            case HvaultColumnFloatVal:
                if (tupdesc->attrs[attnum]->atttypid != FLOAT8OID)
                    ereport(ERROR, 
                            (errcode(ERRCODE_FDW_ERROR),
                             errmsg("Invalid column type %d", attnum),
                             errhint("SDS column must have float8 type")));
                break;
            case HvaultColumnPoint:
            case HvaultColumnFootprint:
                if (tupdesc->attrs[attnum]->atttypid != geomtypeoid)
                    ereport(ERROR, 
                            (errcode(ERRCODE_FDW_ERROR),
                             errmsg("Invalid column type %d", attnum),
                             errhint(
                        "Point & footprint columns must have geometry type")));
                break;
            case HvaultColumnFileIdx:
            case HvaultColumnLineIdx:
            case HvaultColumnSampleIdx:
                if (tupdesc->attrs[attnum]->atttypid != INT4OID)
                    ereport(ERROR, 
                            (errcode(ERRCODE_FDW_ERROR),
                             errmsg("Invalid column type %d", attnum),
                             errhint("Index column must have int4 type")));
                break;
            case HvaultColumnTime:
                if (tupdesc->attrs[attnum]->atttypid != TIMESTAMPOID)
                    ereport(ERROR, 
                            (errcode(ERRCODE_FDW_ERROR),
                             errmsg("Invalid column type %d", attnum),
                             errhint("Time column must have timestamp type")));
                break;
            case HvaultColumnNull:
                /* nop */
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                                errmsg("Undefined column type")));
        }
        attnum++;
    }
}

static void 
calc_next_footprint(HvaultExecState const *scan, HvaultHDFFile *file)
{
    int scan_line;
    Assert(scan->lat != NULL);
    Assert(scan->lon != NULL);
    Assert(scan->lat->type == DFNT_FLOAT32);
    Assert(scan->lon->type == DFNT_FLOAT32);
    /*
     * Three possibilities:
     * 1. first line in scan -> extrapolate prev, interpolate next
     * 2. last line in scan  -> reuse prev,       extrapolate next
     * 3. casual line        -> reuse prev,       interpolate next
     */
    scan_line = scan->cur_line % scan->scan_size;
    if (scan_line == 0)
    {
        /* extrapolate prev */
        extrapolate_line(file->num_samples, 
                         (float *) scan->lat->next, 
                         (float *) scan->lat->cur, 
                         file->prevbrdlat);
        extrapolate_line(file->num_samples, 
                         (float *) scan->lon->next, 
                         (float *) scan->lon->cur, 
                         file->prevbrdlon);
    }
    else 
    {
        /* reuse prev */
        float *buf;
        
        buf = file->prevbrdlat;
        file->prevbrdlat = file->nextbrdlat;
        file->nextbrdlat = buf;

        buf = file->prevbrdlon;
        file->prevbrdlon = file->nextbrdlon;
        file->nextbrdlon = buf;
    }

    if (scan_line == scan->scan_size - 1)
    {
        /* extrapolate next */
        extrapolate_line(file->num_samples, 
                         (float *) scan->lat->prev, 
                         (float *) scan->lat->cur, 
                         file->nextbrdlat);
        extrapolate_line(file->num_samples, 
                         (float *) scan->lon->prev, 
                         (float *) scan->lon->cur, 
                         file->nextbrdlon);
    }
    else 
    {
        /* interpolate next */
        interpolate_line(file->num_samples, 
                         (float *) scan->lat->cur,
                         (float *) scan->lat->next, 
                         file->nextbrdlat);
        interpolate_line(file->num_samples, 
                         (float *) scan->lon->cur,
                         (float *) scan->lon->next, 
                         file->nextbrdlon);
    }
}

static void 
fetch_next_line(HvaultExecState *scan)
{
    ListCell *l;

    foreach(l, scan->file.sds)
    {
        HvaultSDSBuffer *sds = (HvaultSDSBuffer *) lfirst(l);
        int32_t start[2], stride[2], end[2];
        void *buf;

        start[0] = scan->cur_line;
        start[1] = 0;
        stride[0] = 1;
        stride[1] = 1;
        end[0] = 1;
        end[1] = scan->file.num_samples;

        if (sds->haswindow)
        {
            if (scan->cur_line == 0) {
                if (SDreaddata(sds->id, start, stride, end, sds->cur) == FAIL)
                {
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                                    errmsg("Can't read data from HDF file")));
                    return; /* will never reach here */
                }
            } 
            else
            {
                /* swap buffers */
                buf = sds->prev;
                sds->prev = sds->cur;
                sds->cur = sds->next;
                sds->next = buf;
            }

            start[0]++;
            buf = sds->next;
        }
        else
        {
            buf = sds->cur;
        }

        /* don't read next line for last line */
        if (start[0] < scan->file.num_lines)
        {
            if (SDreaddata(sds->id, start, stride, end, buf) == FAIL)
            {
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                                errmsg("Can't read data from HDF file")));
                return; /* will never reach here */       
            }    
        }
    }

    /* Calculate footprint data */
    if (scan->has_footprint)
    {
        calc_next_footprint(scan, &scan->file);
    }
}

static inline void
fill_float_val(HvaultExecState const *scan, AttrNumber attnum)
{
    HvaultSDSBuffer *sds = scan->colbuffer[attnum];
    if (sds->fill_val != NULL && 
        hdf_cmp(sds->type, sds->cur, scan->cur_sample, sds->fill_val))
    {
        scan->nulls[attnum] = true;
    } 
    else
    {
        scan->nulls[attnum] = false;
        scan->sds_vals[attnum] = hdf_value(
            sds->type, sds->cur, scan->cur_sample) / sds->scale - sds->offset;
        scan->values[attnum] = Float8GetDatumFast(scan->sds_vals[attnum]);
    }
}

static inline void
fill_point_val(HvaultExecState const *scan, AttrNumber attnum)
{
    /* This is quite dirty code that uses internal representation of LWPOINT.
       However it is a very hot place here */
    GSERIALIZED *ret;
    double *data = (double *) scan->point->point->serialized_pointlist;
    data[0] = hdf_value(scan->lat->type, scan->lat->cur, scan->cur_sample);
    data[1] = hdf_value(scan->lon->type, scan->lon->cur, scan->cur_sample);
    ret = gserialized_from_lwgeom((LWGEOM *) scan->point, true, NULL);
    scan->nulls[attnum] = false;
    scan->values[attnum] = PointerGetDatum(ret);
}

static inline void
fill_footprint_val(HvaultExecState const *scan, AttrNumber attnum)
{
    /* This is quite dirty code that uses internal representation of LWPOLY.
       However it is a very hot place here */
    GSERIALIZED *ret;
    double *data = (double *) scan->poly->rings[0]->serialized_pointlist;
    data[0] = scan->file.prevbrdlat[scan->cur_sample];
    data[1] = scan->file.prevbrdlon[scan->cur_sample];
    data[2] = scan->file.prevbrdlat[scan->cur_sample+1];
    data[3] = scan->file.prevbrdlon[scan->cur_sample+1];
    data[4] = scan->file.nextbrdlat[scan->cur_sample+1];
    data[5] = scan->file.nextbrdlon[scan->cur_sample+1];
    data[6] = scan->file.nextbrdlat[scan->cur_sample];
    data[7] = scan->file.nextbrdlon[scan->cur_sample];
    data[8] = scan->file.prevbrdlat[scan->cur_sample];
    data[9] = scan->file.prevbrdlon[scan->cur_sample];
    lwgeom_calculate_gbox((LWGEOM *) scan->poly, scan->poly->bbox);
    ret = gserialized_from_lwgeom((LWGEOM *) scan->poly, true, NULL);
    scan->nulls[attnum] = false;
    scan->values[attnum] = PointerGetDatum(ret);   
}

static inline void 
fill_tuple(HvaultExecState const *scan)
{
    ListCell *l;
    AttrNumber attnum = 0;
    foreach(l, scan->coltypes)
    {
        HvaultColumnType type = lfirst_int(l);
        switch(type)
        {
            case HvaultColumnNull:
                scan->nulls[attnum] = true;
                break;
            case HvaultColumnFloatVal:
                fill_float_val(scan, attnum);
                break;
            case HvaultColumnPoint:
                fill_point_val(scan, attnum);
                break;
            case HvaultColumnFootprint:
                fill_footprint_val(scan, attnum);
                break;
            case HvaultColumnFileIdx:
                scan->values[attnum] = Int32GetDatum(scan->cur_file);
                scan->nulls[attnum] = false;
                break;
            case HvaultColumnLineIdx:
                scan->values[attnum] = Int32GetDatum(scan->cur_line);
                scan->nulls[attnum] = false;
                break;
            case HvaultColumnSampleIdx:
                scan->values[attnum] = Int32GetDatum(scan->cur_sample);
                scan->nulls[attnum] = false;
                break;
            case HvaultColumnTime:
                scan->nulls[attnum] = scan->file_time == 0;
                if (!scan->nulls[attnum])
                {
                    scan->values[attnum] = TimestampGetDatum(scan->file_time);
                }
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                                errmsg("Undefined column type %d", type)));
                return; /* Will never reach here */
        }
        attnum++;
    }
}

static bool
fetch_next_file(HvaultExecState *scan)
{
    Portal file_cursor;
    bool retval = false;

    elog(DEBUG1, "fetching next file");
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't connect to SPI")));
        return false; /* Will never reach this */
    }

    if (scan->cursor->file_cursor_name == NULL)
    {
        startCatalogCursor(scan->cursor, scan->fdw_expr, scan->expr_ctx);
    } 
    else 
    {
        /* Close previous file */
        hdf_file_close(&scan->file);
    }
    
    file_cursor = SPI_cursor_find(scan->cursor->file_cursor_name);
    Assert(file_cursor);
    do 
    {
        Datum val;
        bool isnull;
        char *filename;

        SPI_cursor_fetch(file_cursor, true, 1);
        if (SPI_processed != 1 || SPI_tuptable == NULL)
        {
            /* Can't fetch more files */
            elog(DEBUG1, "No more files");
            break;
        }   

        if (SPI_tuptable->tupdesc->natts != 3 ||
            SPI_tuptable->tupdesc->attrs[0]->atttypid != INT4OID ||
            SPI_tuptable->tupdesc->attrs[1]->atttypid != TEXTOID ||
            SPI_tuptable->tupdesc->attrs[2]->atttypid != TIMESTAMPOID)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Error in catalog query")));
            return false; /* Will never reach this */         
        }

        val = heap_getattr(SPI_tuptable->vals[0], 1, 
                           SPI_tuptable->tupdesc, &isnull);
        if (isnull)
        {
            elog(WARNING, "Wrong entry in catalog, skipping");
            continue;
        }
        scan->cur_file = DatumGetInt32(val);
        filename = SPI_getvalue(SPI_tuptable->vals[0], 
                                SPI_tuptable->tupdesc, 2);
        if (!filename)
        {
            elog(WARNING, "Wrong entry in catalog, skipping");
            continue;
        }
        val = heap_getattr(SPI_tuptable->vals[0], 3, 
                           SPI_tuptable->tupdesc, &isnull);
        scan->file_time = isnull ? 0 : DatumGetTimestamp(val);

        retval = hdf_file_open(&scan->file, filename, scan->has_footprint);
    }
    while(retval == false);

    if (SPI_finish() != SPI_OK_FINISH)    
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't finish access to SPI")));
        return false; /* Will never reach this */   
    }

    return retval;
}

static HvaultCatalogCursor *
makeCatalogCursor(char const *query, List *fdw_expr)
{
    HvaultCatalogCursor *cursor;
    ListCell *l;

    cursor = palloc(sizeof(HvaultCatalogCursor));

    cursor->query = query;
    cursor->file_cursor_name = NULL;
    cursor->prep_stmt = NULL;
    cursor->argtypes = NULL;
    cursor->argvals = NULL;
    cursor->argnulls = NULL;

    int pos = 0;
    int nargs = list_length(fdw_expr);
    cursor->argtypes = palloc(sizeof(Oid) * nargs);
    cursor->argvals = palloc(sizeof(Datum) * nargs);
    cursor->argnulls = palloc(sizeof(char) * nargs);
    foreach(l, fdw_expr)
    {
        Expr *expr = (Expr *) lfirst(l);
        cursor->argtypes[pos] = exprType((Node *) expr);
        pos++;
    }

    return cursor;
}

static void 
startCatalogCursor(HvaultCatalogCursor *cursor, 
                   List *fdw_expr, 
                   ExprContext *expr_ctx)
{
    ListCell *l;
    int nargs, pos;
    Portal file_cursor;
        
    Assert(cursor->query);
    nargs = list_length(fdw_expr);

    elog(DEBUG1, "file cursor initialization");
    if (cursor->prep_stmt == NULL)
    {
        cursor->prep_stmt = SPI_prepare(cursor->query, nargs, cursor->argtypes);
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

    pos = 0;
    foreach(l, fdw_expr)
    {
        bool isnull;
        Assert(IsA(lfirst(l), ExprState));
        ExprState *expr = (ExprState *) lfirst(l);
        cursor->argvals[pos] = ExecEvalExpr(expr, expr_ctx, 
                                            &isnull, NULL);
        cursor->argnulls[pos] = isnull ? 'n' : ' ';
        pos++;
    }

    file_cursor = SPI_cursor_open(NULL, cursor->prep_stmt, cursor->argvals, 
                                  cursor->argnulls, true);
    cursor->file_cursor_name = file_cursor->name;
}

static void
initHDFFile(HvaultHDFFile *file, MemoryContext memctx)
{
    MemoryContext file_cxt = AllocSetContextCreate(memctx,
                                                   "hvault_fdw per-file data",
                                                   ALLOCSET_DEFAULT_MINSIZE,
                                                   ALLOCSET_DEFAULT_INITSIZE,
                                                   ALLOCSET_DEFAULT_MAXSIZE);
    file->filememcxt = file_cxt;
    file->filename = NULL;
    file->sds = NIL;
    file->prevbrdlat = NULL;
    file->prevbrdlon = NULL;
    file->nextbrdlat = NULL;
    file->nextbrdlon = NULL;
    file->sel = NULL;
    file->sel_size = -1;
    file->sd_id = FAIL;
    file->num_lines = -1;
    file->num_samples = -1;
    file->open_time = 0;
}

static HvaultGeomPredicate *
listToPredicate(List *p)
{
    Assert(list_length(p) == HvaultPredicateNumParams);
    HvaultGeomPredicate *res = palloc(sizeof(HvaultGeomPredicate));
    res->coltype = list_nth_int(p, HvaultPredicateColtype);
    res->op      = list_nth_int(p, HvaultPredicateGeomOper);
    res->argno   = list_nth_int(p, HvaultPredicateArgno);
    res->isneg   = list_nth_int(p, HvaultPredicateIsNegative);
    return res;
}

static HvaultExecState *
makeExecState(List *coltypes, 
              MemoryContext memctx, 
              List *fdw_expr, 
              PlanState *plan,
              List *predicates)
{
    ListCell *l;
    HvaultExecState *state = palloc(sizeof(HvaultExecState));

    state->natts = list_length(coltypes);
    state->coltypes = coltypes;
    state->has_footprint = false;
    state->scan_size = 10;

    state->expr_ctx = plan->ps_ExprContext;
    state->fdw_expr = NIL;
    foreach(l, fdw_expr)
    {
        Expr *expr = (Expr *) lfirst(l);
        state->fdw_expr = lappend(state->fdw_expr, ExecInitExpr(expr, plan));
    }

    state->predicates = NIL;
    foreach(l, predicates)    
    {
        List *p = (List *) lfirst(l);
        state->predicates = lappend(state->predicates, listToPredicate(p));
    }

    state->cursor = NULL;
    initHDFFile(&state->file, memctx);
    state->colbuffer = palloc0(sizeof(HvaultSDSBuffer *) * state->natts);
    state->lat = NULL;
    state->lon = NULL;

    state->file_time = 0;
    state->cur_file = -1;
    state->cur_line = -1;
    state->cur_sample = -1;
    state->cur_sel = -1;

    state->values = palloc(sizeof(Datum) * state->natts);
    state->nulls = palloc(sizeof(bool) * state->natts);
    state->sds_vals = palloc(sizeof(double) * state->natts);

    state->point = lwpoint_make2d(SRID_UNKNOWN, 0, 0);
    state->ptarray = ptarray_construct(false, false, 5);
    state->poly = lwpoly_construct(SRID_UNKNOWN, NULL, 1, &state->ptarray);
    lwgeom_add_bbox(lwpoly_as_lwgeom(state->poly));

    return state;
}

static char *
getColumnSDS(Oid relid, AttrNumber attnum, TupleDesc tupdesc)
{
    List *options;
    ListCell *o;

    Assert(attnum < tupdesc->natts);
    options = GetForeignColumnOptions(relid, attnum+1);
    elog(DEBUG1, "options for att %d: %s", attnum, nodeToString(options));
    foreach(o, options)
    {
        DefElem *def = (DefElem *) lfirst(o);
        if (strcmp(def->defname, "sds") == 0) {
            return defGetString(def);
        }
    }

    elog(DEBUG1, "Can't find sds option for att: %d relid: %d opts: %s", 
         attnum, relid, nodeToString(options));
    return NameStr(tupdesc->attrs[attnum]->attname);;
}

static HvaultSDSBuffer *
getSDSBuffer(List **buffers, char *name)
{
    ListCell *l;
    HvaultSDSBuffer *sds = NULL;

    elog(DEBUG1, "creating buffer for sds %s", name);

    foreach(l, *buffers)
    {
        sds = (HvaultSDSBuffer *) lfirst(l);
        if (strcmp(name, sds->name) == 0) 
            return sds;
    }

    sds = palloc(sizeof(HvaultSDSBuffer));
    sds->prev = NULL;
    sds->cur = NULL;
    sds->next = NULL;
    sds->haswindow = false;
    sds->id = -1;
    sds->name = name;
    sds->type = -1;
    *buffers = lappend(*buffers, sds);
    return sds;
}

static void
assignSDSBuffers(HvaultExecState *state, Oid relid, TupleDesc tupdesc)
{
    /* fill required sds list & helper pointers */
    ListCell *l;
    AttrNumber attnum = 0;
    foreach(l, state->coltypes)
    {
        switch(lfirst_int(l)) {
            case HvaultColumnFloatVal:
                state->colbuffer[attnum] = getSDSBuffer(
                    &(state->file.sds), 
                    getColumnSDS(relid, attnum, tupdesc));
                break;
            case HvaultColumnPoint:
                state->lat = getSDSBuffer(&(state->file.sds), "Latitude");
                state->lon = getSDSBuffer(&(state->file.sds), "Longitude");
                break;
            case HvaultColumnFootprint:
                state->lat = getSDSBuffer(&(state->file.sds), "Latitude");
                state->lat->haswindow = true;
                state->lon = getSDSBuffer(&(state->file.sds), "Longitude");
                state->lon->haswindow = true;
                state->has_footprint = true;
                break;
        }
        attnum++;
    }
    if (list_length(state->file.sds) == 0)
    {
        /* Adding latitude column to get size of files */
        getSDSBuffer(&(state->file.sds), "Latitude");
    }
    elog(DEBUG1, "SDS buffers: %d", list_length(state->file.sds));
}

