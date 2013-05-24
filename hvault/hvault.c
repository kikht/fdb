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
static HvaultGeomPredicate *listToPredicate(List *p);
static HvaultExecState *makeExecState(List *coltypes, 
                                      MemoryContext memctx, 
                                      List *fdw_expr, 
                                      PlanState *plan,
                                      List *predicates);
static void assignSDSBuffers(HvaultExecState *state, 
                             Oid relid, 
                             TupleDesc tupdesc);
static void  check_column_types(List *coltypes, TupleDesc tupdesc);

/* Iteration functions */
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

    ForeignScan *plan;
    List *fdw_plan_private;
    Oid foreigntableid;
    char *catalog;
    List *predicates, *pred_str;
    ListCell *l;

    plan = (ForeignScan *) node->ss.ps.plan;
    fdw_plan_private = plan->fdw_private;
    foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
    catalog = hvaultGetTableOptionString(foreigntableid, "catalog");
    if (catalog)
    {
        Value *query = list_nth(fdw_plan_private, HvaultPlanCatalogQuery);
        ExplainPropertyText("Catalog query", strVal(query), es);
    }
    else
    {
        char *filename = hvaultGetTableOptionString(foreigntableid, "filename");
        if (filename != NULL)
            ExplainPropertyText("Single file", filename, es);
    }

    predicates = list_nth(fdw_plan_private, HvaultPlanPredicates);
    pred_str = NIL;
    foreach(l, predicates)
    {
        HvaultGeomPredicate *pred = listToPredicate((List *) lfirst(l));
        StringInfoData str;
        char *colname;
        initStringInfo(&str);

        switch (pred->coltype) {
            case HvaultColumnFootprint:
                colname = "footprint";
                break;
            case HvaultColumnPoint:
                colname = "point";
                break;
            default:
                colname = "<unknown>";
        }

        if (pred->isneg)
            appendStringInfoString(&str, "NOT ");
    
        if (pred->op < HvaultGeomNumRealOpers) 
        {
            appendStringInfo(&str, "%s %s $%d", 
                             colname, geomopstr[pred->op], pred->argno+1);
        }
        else
        {
            appendStringInfo(&str, "$%d %s %s", 
                             pred->argno+1, geomopstr[pred->op], colname);
        }
        pred_str = lappend(pred_str, str.data);

        pfree(pred);
    }
    if (list_length(pred_str) > 0)
        ExplainPropertyList("Geometry predicates", pred_str, es);
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

    catalog = hvaultGetTableOptionString(foreigntableid, "catalog");
    if (catalog)
    {
        List *query = list_nth(fdw_plan_private, HvaultPlanCatalogQuery);
        state->cursor = hvaultCatalogInitCursor(query);
    }
    else
    {
        state->file.filename = hvaultGetTableOptionString(foreigntableid, 
                                                          "filename");
        if (!state->file.filename)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't find catalog or filename option")));
        }
    }

    /* fill required sds list & helper pointers */
    assignSDSBuffers(state, foreigntableid, tupdesc);
    state->shift_longitude = hvaultGetTableOptionBool(foreigntableid, 
                                                      "shift_longitude");
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
            GSERIALIZED *arggeom = (GSERIALIZED*) PG_DETOAST_DATUM(argdatum);
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
    hvaultCatlogResetCursor(state->cursor);
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
        hvaultCatalogFreeCursor(state->cursor);
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


    catalog = hvaultGetTableOptionString(RelationGetRelid(relation), "catalog");
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
    Oid foreigntableid;
    TupleDesc tupdesc;
    int num_rows = 0;
    int num_files;
    double rowstoskip = -1;
    double rstate = anl_init_selection_state(targrows);
    double procrows = 0;

    HvaultColumnType * coltypes;
    AttrNumber natts, attnum;
    HvaultCatalogQuery query;
    List * coltypes_list = NIL;

    foreigntableid = RelationGetRelid(relation);
    tupdesc = RelationGetDescr(relation);
    natts = RelationGetNumberOfAttributes(relation);
    catalog = hvaultGetTableOptionString(foreigntableid, "catalog");
    if (catalog == NULL)
        return 0;

    coltypes = palloc(sizeof(HvaultColumnType) * natts);
    for (attnum = 0; attnum < natts; ++attnum)
    {
        HvaultColumnType type = hvaultGetColumnType(foreigntableid, attnum+1);
        coltypes[attnum] = type;
        coltypes_list = lappend_int(coltypes_list, type);
    }
    state = makeExecState(coltypes_list, CurrentMemoryContext, NIL, NULL, NIL);

    num_files = targrows / 100;
    if (num_files < 5)
        num_files = 5;

    query = hvaultCatalogInitQuery(catalog, coltypes, natts);
    hvaultCatalogAddProduct(query, "file");
    //TODO: add all necessary products (loop through columns)
    hvaultCatalogAddSortQual(query, "random()");
    hvaultCatalogAddLimitQual(query, num_files);
    state->cursor = hvaultCatalogInitCursor(hvaultCatalogPackQuery(query));
    
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
            case HvaultColumnInt8Val:
                if (tupdesc->attrs[attnum]->atttypid != INT2OID)
                    ereport(ERROR, 
                            (errcode(ERRCODE_FDW_ERROR),
                             errmsg("Invalid column type %d", attnum),
                             errhint("SDS column must have char type")));
                break;
            case HvaultColumnInt16Val:
                if (tupdesc->attrs[attnum]->atttypid != INT2OID)
                    ereport(ERROR, 
                            (errcode(ERRCODE_FDW_ERROR),
                             errmsg("Invalid column type %d", attnum),
                             errhint("SDS column must have int2 type")));
                break;
            case HvaultColumnInt32Val:
                if (tupdesc->attrs[attnum]->atttypid != INT4OID)
                    ereport(ERROR, 
                            (errcode(ERRCODE_FDW_ERROR),
                             errmsg("Invalid column type %d", attnum),
                             errhint("SDS column must have int4 type")));
                break;
            case HvaultColumnInt64Val:
                if (tupdesc->attrs[attnum]->atttypid != INT8OID)
                    ereport(ERROR, 
                            (errcode(ERRCODE_FDW_ERROR),
                             errmsg("Invalid column type %d", attnum),
                             errhint("SDS column must have int8 type")));
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

static void shift_longitude_line(float *buf, int size)
{
    int i;
    for (i = 0; i < size; ++i)
    {
        buf[i] += (float)(360 * (buf[i] < 0));
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
            if (scan->cur_line == 0) 
            {
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

    if (scan->shift_longitude && scan->lon != NULL) 
    {
        if (scan->lon->haswindow)
        {
            if (scan->cur_line == 0) 
                shift_longitude_line(scan->lon->cur, scan->file.num_samples);

            if (scan->cur_line != scan->file.num_lines - 1)
                shift_longitude_line(scan->lon->next, scan->file.num_samples);                
        }
        else 
        {
            shift_longitude_line(scan->lon->cur, scan->file.num_samples);
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
    scan->nulls[attnum] = hdf_cmp(sds->type, sds->cur, scan->cur_sample, 
                                  sds->fill_val);
    if (!scan->nulls[attnum]) 
    {
        scan->sds_floats[attnum] = hdf_value(
            sds->type, sds->cur, scan->cur_sample) / sds->scale - sds->offset;
        scan->values[attnum] = Float8GetDatumFast(scan->sds_floats[attnum]);    
    }
    
}

static inline void
fill_int8_val(HvaultExecState const *scan, AttrNumber attnum)
{
    HvaultSDSBuffer *sds = scan->colbuffer[attnum];
    scan->nulls[attnum] = hdf_cmp(sds->type, sds->cur, scan->cur_sample, 
                                  sds->fill_val);
    if (!scan->nulls[attnum]) 
    {
        scan->values[attnum] = 
            Int16GetDatum(((int8_t *) sds->cur)[scan->cur_sample]);
    }
}

static inline void
fill_int16_val(HvaultExecState const *scan, AttrNumber attnum)
{
    HvaultSDSBuffer *sds = scan->colbuffer[attnum];
    scan->nulls[attnum] = hdf_cmp(sds->type, sds->cur, scan->cur_sample, 
                                  sds->fill_val);
    if (!scan->nulls[attnum]) 
    {
        scan->values[attnum] = 
            Int16GetDatum(((int16_t *) sds->cur)[scan->cur_sample]);
    }
}

static inline void
fill_int32_val(HvaultExecState const *scan, AttrNumber attnum)
{
    HvaultSDSBuffer *sds = scan->colbuffer[attnum];
    scan->nulls[attnum] = hdf_cmp(sds->type, sds->cur, scan->cur_sample, 
                                  sds->fill_val);
    if (!scan->nulls[attnum]) 
    {
        scan->values[attnum] = 
            Int32GetDatum(((int32_t *) sds->cur)[scan->cur_sample]);
    }
}

static inline void
fill_int64_val(HvaultExecState const *scan, AttrNumber attnum)
{
    HvaultSDSBuffer *sds = scan->colbuffer[attnum];
    scan->nulls[attnum] = hdf_cmp(sds->type, sds->cur, scan->cur_sample, 
                                  sds->fill_val);
    if (!scan->nulls[attnum]) 
    {
        scan->sds_ints[attnum] = ((int64_t *) sds->cur)[scan->cur_sample];
        scan->values[attnum] = Int64GetDatumFast(scan->sds_ints[attnum]);
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
            case HvaultColumnInt8Val:
                fill_int8_val(scan, attnum);
                break;
            case HvaultColumnInt16Val:
                fill_int16_val(scan, attnum);
                break;
            case HvaultColumnInt32Val:
                fill_int32_val(scan, attnum);
                break;
            case HvaultColumnInt64Val:
                fill_int64_val(scan, attnum);
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

static void 
startCatalogCursor (HvaultCatalogCursor cursor, 
                    ListCell * fdw_expr, 
                    int num_exprs,
                    ExprContext * expr_ctx)
{
    ListCell *l;
    int nargs, pos;
    Oid * argtypes;
    Datum * argvals;
    char * argnulls;

    argtypes = palloc(nargs * sizeof(Oid));
    argvals = palloc(nargs * sizeof(Datum));
    argnulls = palloc(nargs * sizeof(char));

    for (pos = 0; pos < num_exprs; pos++, fdw_expr = lnext(fdw_expr)) 
    {
        ExprState *expr;
        bool isnull;   

        Assert(fdw_expr);
        expr = (ExprState *) lfirst(fdw_expr);
        Assert(IsA(expr, ExprState));
        argvals[pos] = ExecEvalExpr(expr, expr_ctx, &isnull, NULL);
        argnulls[pos] = isnull ? 'n' : ' ';
    }

    hvaultCatalogStartCursor(cursor, argtypes, argvals, argnulls);

    pfree(argtypes);
    pfree(argvals);
    pfree(argnulls);
}

static bool
fetch_next_file(HvaultExecState *scan)
{
    HvaultCatalogCursorResult res;
    char const * filename;

    Assert(scan->cursor);
    
    res = hvaultCatalogNext(scan->cursor);
    switch (res) 
    {
        case HvaultCatalogCursorEOF:
            /* Can't fetch more files */
            elog(DEBUG1, "No more files");
            return false;
        case HvaultCatalogCursorNotStarted:
            //TODO: use exact number of parameters instead of full list
            startCatalogCursor(scan->cursor, 
                               list_head(scan->fdw_expr),
                               list_length(scan->fdw_expr),
                               scan->expr_ctx);
            break;
        case HvaultCatalogCursorOK:
            hdf_file_close(&scan->file);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Unexpected cursor retval")));
            return false; /* Will never reach this */                
    }
    scan->cur_file = hvaultCatalogGetId(scan->cursor);
    scan->file_time = hvaultCatalogGetStarttime(scan->cursor);
    filename = hvaultCatalogGetFilename(scan->cursor, "file");
    return hdf_file_open(&scan->file, filename, scan->has_footprint);
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
    HvaultGeomPredicate *res;

    Assert(list_length(p) == HvaultPredicateNumParams);
    res = palloc(sizeof(HvaultGeomPredicate));
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
    state->shift_longitude = false;
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
    state->sds_floats = palloc(sizeof(double) * state->natts);
    state->sds_ints = palloc(sizeof(int64_t) * state->natts);

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
            case HvaultColumnInt8Val:
            case HvaultColumnInt16Val:
            case HvaultColumnInt32Val:
            case HvaultColumnInt64Val:
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
