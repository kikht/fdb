#include "hvault.h"

/* PostgreSQL */
#include <access/skey.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <executor/spi.h>
#include <foreign/foreign.h>
#include <nodes/bitmapset.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/planmain.h>
#include <optimizer/restrictinfo.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/rel.h>
#include <utils/timestamp.h>

/* PostGIS */
#include <liblwgeom.h>

/* HDF */
#include <hdf/mfhdf.h>
#include <hdf/hlimits.h>


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

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
    /*fdwroutine->AnalyzeForeignTable = hvaultAnalyze;*/

    PG_RETURN_POINTER(fdwroutine);
}

PG_FUNCTION_INFO_V1(hvault_fdw_validator);
Datum 
hvault_fdw_validator(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(true);
}

static void 
hvaultGetRelSize(PlannerInfo *root, 
                 RelOptInfo *baserel, 
                 Oid foreigntableid)
{
    HvaultPlanState *fdw_private;
    double num_files, tuples_per_file, scale_factor;
    

    fdw_private = (HvaultPlanState *) palloc(sizeof(HvaultPlanState));
    baserel->fdw_private = fdw_private;

    /* We can build estimate basing on:
     * baserel->reltargetlist - List of Var and PlaceHolderVar nodes for 
     *      the values we need to output from this relation. 
     * baserel->baserestrictinfo - 
     *      List of RestrictInfo nodes, containing info about
     *      each non-join qualification clause in which this relation
     *      participates 
     * baserel->joininfo - 
     *      List of RestrictInfo nodes, containing info about each
     *      join clause in which this relation participates 
     *
     * Don't forget about equivalence classes!
     */    

    fdw_private->coltypes = get_column_types(baserel, foreigntableid);
    fdw_private->catalog = get_table_option(foreigntableid, "catalog");
    baserel->width = get_row_width(fdw_private);
    num_files = get_num_files(fdw_private);
    tuples_per_file = TUPLES_PER_FILE;
    scale_factor = 1; /* 4 for 500m, 16 for 250m */
    baserel->rows = num_files * tuples_per_file * scale_factor ; 

    elog(DEBUG1, "GetRelSize: baserestrictinfo: %s joininfo: %s",
         nodeToString(baserel->baserestrictinfo),
         nodeToString(baserel->joininfo));
}

static void 
hvaultGetPaths(PlannerInfo *root, 
               RelOptInfo *baserel,
               Oid foreigntableid)
{
    HvaultPlanState *fdw_private = (HvaultPlanState *) baserel->fdw_private;
    ForeignPath *path = NULL;
    double rows;           /* estimate number of rows returned by path */
    Cost startup_cost, total_cost;  
    List *pathkeys = NIL;  /* represent a pre-sorted result */
    Relids required_outer = NULL; /* Bitmap of rels supplying parameters used 
                                     by path. Look at the baserel.joininfo and 
                                     equivalence classes to generate possible 
                                     parametrized paths*/
    List *fdw_path_private = NIL;/* best practice is to use a representation 
                               that's dumpable by nodeToString, for use with 
                               debugging support available in the backend. 
                               List of DefElem suits well for this*/
    
    rows = baserel->rows;
    startup_cost = 10;
    /* TODO: files * lines_per_file * line_cost(num_sds, has_footprint) */
    total_cost = rows * 5; 
    if (has_useful_pathkeys(root, baserel))
    {
        /* TODO: time sort */
        pathkeys = get_sort_pathkeys(root, baserel);
    }
    fdw_path_private = fdw_private->coltypes;

    /*
    bool add_path_precheck(RelOptInfo *parent_rel,
                      Cost startup_cost, Cost total_cost,
                      List *pathkeys, Relids required_outer)
    Can be useful to reject uninteresting paths before creating path struct
    */ 
    path = create_foreignscan_path(root, 
                                   baserel, 
                                   rows, 
                                   startup_cost, 
                                   total_cost, 
                                   pathkeys, 
                                   required_outer, 
                                   fdw_path_private);
    add_path(baserel, (Path *) path);
}

static ForeignScan *
hvaultGetPlan(PlannerInfo *root, 
              RelOptInfo *baserel,
              Oid foreigntableid, 
              ForeignPath *best_path,
              List *tlist, 
              List *scan_clauses)
{
    List *fdw_path_private = best_path->fdw_private; 
    List *rest_clauses; /* clauses that must be checked externally */
    /* Both of these lists must be represented in a form that 
       copyObject knows how to copy */
    List *fdw_exprs = NIL; /* expressions that will be needed inside scan
                              these include right parts of the restriction 
                              clauses and right parts of parametrized
                              join clauses */
    List *fdw_plan_private = NIL;

    /* minimal effort to get rest clauses & fdw_exprs*/
    rest_clauses = extract_actual_clauses(scan_clauses, false);
    fdw_exprs = NIL;

    elog(DEBUG1, "GetPlan: scan_cl: %s rest_cl: %s",
         nodeToString(scan_clauses),
         nodeToString(rest_clauses));
    elog(DEBUG1, "GetPlan: tlist: %s", nodeToString(tlist));

    fdw_plan_private = fdw_path_private;

    return make_foreignscan(tlist, rest_clauses, baserel->relid, fdw_exprs, 
                            fdw_plan_private);
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
}

static void 
hvaultBegin(ForeignScanState *node, int eflags)
{
    HvaultExecState *fdw_exec_state;
    Oid foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
    ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
    List *fdw_plan_private = plan->fdw_private;
    /*List *fdw_exprs = plan->fdw_exprs;*/
    TupleDesc tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
    ListCell *l;
    AttrNumber attnum;
    MemoryContext file_cxt;


    elog(DEBUG1, "in hvaultBegin");
    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    file_cxt = AllocSetContextCreate(node->ss.ps.state->es_query_cxt,
                                     "hvault_fdw per-file data",
                                     ALLOCSET_DEFAULT_MINSIZE,
                                     ALLOCSET_DEFAULT_INITSIZE,
                                     ALLOCSET_DEFAULT_MAXSIZE);

    fdw_exec_state = (HvaultExecState *) palloc(sizeof(HvaultExecState));
    fdw_exec_state->natts = tupdesc->natts;
    fdw_exec_state->colbuffer = palloc0(sizeof(HvaultSDSBuffer *) * 
                                        fdw_exec_state->natts);
    fdw_exec_state->coltypes = fdw_plan_private;
    fdw_exec_state->values = palloc(sizeof(Datum) * fdw_exec_state->natts);
    fdw_exec_state->nulls = palloc(sizeof(bool) * fdw_exec_state->natts);
    fdw_exec_state->file.filememcxt = file_cxt;
    fdw_exec_state->file.sd_id = FAIL;
    fdw_exec_state->file.num_lines = -1;
    fdw_exec_state->file.num_samples = -1;
    fdw_exec_state->file.prevbrdlat = NULL;
    fdw_exec_state->file.prevbrdlon = NULL;
    fdw_exec_state->file.nextbrdlat = NULL;
    fdw_exec_state->file.nextbrdlon = NULL;
    fdw_exec_state->file.filename = NULL;
    fdw_exec_state->cur_line = -1;
    fdw_exec_state->cur_sample = -1;
    fdw_exec_state->lat = NULL;
    fdw_exec_state->lon = NULL;
    fdw_exec_state->has_footprint = false;
    /* TODO: variable scan size, depending on image scale & user params */
    fdw_exec_state->scan_size = 10;
    fdw_exec_state->catalog = NULL;
    fdw_exec_state->file_cursor_name = NULL;

    check_column_types(fdw_exec_state->coltypes, tupdesc);
    /* fill required sds list & helper pointers */
    fdw_exec_state->file.sds = NIL;
    attnum = 0;
    foreach(l, fdw_exec_state->coltypes)
    {
        switch(lfirst_int(l)) {
            case HvaultColumnFloatVal:
                fdw_exec_state->colbuffer[attnum] = get_sds_buffer(
                    &(fdw_exec_state->file.sds), 
                    get_column_sds(foreigntableid, attnum, tupdesc));
                break;
            case HvaultColumnPoint:
                fdw_exec_state->lat = get_sds_buffer(
                    &(fdw_exec_state->file.sds), "Latitude");
                fdw_exec_state->lon = get_sds_buffer(
                    &(fdw_exec_state->file.sds), "Longitude");
                break;
            case HvaultColumnFootprint:
                fdw_exec_state->lat = get_sds_buffer(
                    &(fdw_exec_state->file.sds), "Latitude");
                fdw_exec_state->lat->haswindow = true;
                fdw_exec_state->lon = get_sds_buffer(
                    &(fdw_exec_state->file.sds), "Longitude");
                fdw_exec_state->lon->haswindow = true;
                fdw_exec_state->has_footprint = true;
                break;
        }
        attnum++;
    }
    if (list_length(fdw_exec_state->file.sds) == 0)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("No SDS columns found")));
    }
    elog(DEBUG1, "SDS buffers: %d", list_length(fdw_exec_state->file.sds));

    fdw_exec_state->catalog = get_table_option(foreigntableid, "catalog");
    if (!fdw_exec_state->catalog)
    {
        fdw_exec_state->file.filename = get_table_option(foreigntableid, 
                                                         "filename");
        if (!fdw_exec_state->file.filename)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't find catalog or filename option")));
        }
    }

    node->fdw_state = fdw_exec_state;
}

static TupleTableSlot *
hvaultIterate(ForeignScanState *node) 
{
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    /*TupleDesc tupdesc = slot->tts_tupleDescriptor;*/
    HvaultExecState *fdw_exec_state = (HvaultExecState *) node->fdw_state;
    ListCell *l;
    AttrNumber attnum;

    elog(DEBUG2, "in hvaultIterate %d:%d:%d", 1, fdw_exec_state->cur_line, 
                                                 fdw_exec_state->cur_sample);

    if (fdw_exec_state->cur_line == 0 && fdw_exec_state->cur_sample == 0)
    {
        elog(DEBUG1, "first hvaultIterate");
    }

    ExecClearTuple(slot);

    /* Next line needed? (initial state -1 == -1) */
    if (fdw_exec_state->cur_sample == fdw_exec_state->file.num_samples)
    {
        fdw_exec_state->cur_sample = 0;
        fdw_exec_state->cur_line++;
        /* Next file needed? */
        if (fdw_exec_state->cur_line >= fdw_exec_state->file.num_lines) {
            if (fdw_exec_state->catalog)
            {
                if (!fetch_next_file(fdw_exec_state))
                {
                    /* End of scan, return empty tuple*/
                    return slot;
                }
            } 
            else 
            {
                /* Single file case */
                /* Initialization or end of file? */
                if (fdw_exec_state->file.sd_id == FAIL)
                {
                    char const *filename = fdw_exec_state->file.filename;
                    if (!hdf_file_open(fdw_exec_state, filename, 
                                       &fdw_exec_state->file))
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
    }

    /* Fill tuple */
    attnum = 0;
    foreach(l, fdw_exec_state->coltypes)
    {
        HvaultColumnType type = lfirst_int(l);
        switch(type)
        {
            case HvaultColumnNull:
                fdw_exec_state->nulls[attnum] = true;
                break;
            case HvaultColumnFloatVal:
                fill_float_val(fdw_exec_state, attnum);
                break;
            case HvaultColumnPoint:
                fill_point_val(fdw_exec_state, attnum);
                break;
            case HvaultColumnFootprint:
                fill_footprint_val(fdw_exec_state, attnum);
                break;
            case HvaultColumnFileIdx:
                fdw_exec_state->values[attnum] = 
                    Int32GetDatum(fdw_exec_state->cur_file);
                fdw_exec_state->nulls[attnum] = false;
                break;
            case HvaultColumnLineIdx:
                fdw_exec_state->values[attnum] = 
                    Int32GetDatum(fdw_exec_state->cur_line);
                fdw_exec_state->nulls[attnum] = false;
                break;
            case HvaultColumnSampleIdx:
                fdw_exec_state->values[attnum] = 
                    Int32GetDatum(fdw_exec_state->cur_sample);
                fdw_exec_state->nulls[attnum] = false;
                break;
            case HvaultColumnTime:
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                                errmsg("Time column is not supported yet")));
                return NULL; /* Will never reach here */
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                                errmsg("Undefined column type %d", type)));
                return NULL; /* Will never reach here */
        }
        attnum++;
    }
    slot->tts_isnull = fdw_exec_state->nulls;
    slot->tts_values = fdw_exec_state->values;
    ExecStoreVirtualTuple(slot);
    fdw_exec_state->cur_sample++;
    return slot;
}

static void 
hvaultReScan(ForeignScanState *node)
{
    HvaultExecState *fdw_exec_state = (HvaultExecState *) node->fdw_state;
    elog(DEBUG1, "in hvaultReScan");
    
    //TODO: cursor reinitialize

    fdw_exec_state->cur_line = 0;
    fdw_exec_state->cur_sample = 0;
    if (fdw_exec_state->file.sd_id != FAIL)
    {
        fetch_next_line(fdw_exec_state);
    }
}


static void 
hvaultEnd(ForeignScanState *node)
{
    HvaultExecState *fdw_exec_state = (HvaultExecState *) node->fdw_state;
    elog(DEBUG1, "in hvaultEnd");
    if (fdw_exec_state == NULL)
        return;
    
    if (fdw_exec_state->file.sd_id != FAIL)
    {
        hdf_file_close(&fdw_exec_state->file);
    }

    if (fdw_exec_state->file_cursor_name != NULL)
    {
        Portal file_cursor = SPI_cursor_find(fdw_exec_state->file_cursor_name);
        SPI_cursor_close(file_cursor);
    }

    MemoryContextDelete(fdw_exec_state->file.filememcxt);
}








static char * 
get_table_option(Oid foreigntableid, char *option)
{
    ListCell *l;
    ForeignTable *foreigntable = GetForeignTable(foreigntableid);
    foreach(l, foreigntable->options)
    {
        DefElem *def = (DefElem *) lfirst(l);
        if (strcmp(def->defname, option) == 0)
        {
            return defGetString(def);
        }
    }
    elog(DEBUG1, "Can't find table option %s", option);
    return NULL;
}


static HvaultColumnType
parse_column_type(char *type) 
{
    if (strcmp(type, "point") == 0) 
    {
        return HvaultColumnPoint;
    }
    else if (strcmp(type, "footprint") == 0)
    {
        return HvaultColumnFootprint;   
    }
    else if (strcmp(type, "file_index") == 0)
    {
        return HvaultColumnFileIdx;
    }
    else if (strcmp(type, "line_index") == 0)
    {
        return HvaultColumnLineIdx;
    }
    else if (strcmp(type, "sample_index") == 0)
    {
        return HvaultColumnSampleIdx;
    }
    else if (strcmp(type, "time") == 0)
    {
        return HvaultColumnTime;
    }
    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
            errmsg("Unknown column type %s", type)));
    return HvaultColumnNull; /* will never reach here */
}

static List *
get_column_types(RelOptInfo *baserel, Oid foreigntableid)
{
    List *res = NIL;
    ListCell *l, *m;
    Relation rel;
    AttrNumber attnum, natts;
    TupleDesc tupleDesc;
    HvaultColumnType *types;
    /* HvaultPlanState *fdw_private = baserel->fdw_private; */

    rel = heap_open(foreigntableid, AccessShareLock);
    tupleDesc = RelationGetDescr(rel);
    natts = tupleDesc->natts;
    heap_close(rel, AccessShareLock);

    types = palloc(sizeof(HvaultColumnType) * natts);
    for (attnum = 0; attnum < natts; attnum++)
    {
        types[attnum] = HvaultColumnNull;
    }

    foreach(l, baserel->reltargetlist)
    {
        Var *var;
        List *colopts;
        
        if (!IsA(lfirst(l), Var))
        {
            elog(WARNING, "Strange object in reltargetlist: %s", 
                 nodeToString(lfirst(l)));
            continue;
        }

        var = (Var *) lfirst(l);
        if (var->varno != baserel->relid)
        {
            elog(WARNING, "Not my var in reltargetlist: %s", nodeToString(var));
            continue;
        }
        Assert(var->varattno <= fdw_private->natts);

        types[var->varattno-1] = HvaultColumnFloatVal;
        colopts = GetForeignColumnOptions(foreigntableid, var->varattno);
        foreach(m, colopts)
        {
            DefElem *opt = (DefElem *) lfirst(m);
            if (strcmp(opt->defname, "type") == 0)
            {
                char *type = defGetString(opt);
                attnum = var->varattno-1;
                types[attnum] = parse_column_type(type);
                elog(DEBUG1, "col: %d strtype: %s type: %d", 
                     attnum, type, types[attnum]);
            }
        }
    }  

    for (attnum = 0; attnum < natts; attnum++)
    {
        res = lappend_int(res, types[attnum]);
    }
    pfree(types);
    return res;
}

static char *
get_column_sds(Oid relid, AttrNumber attnum, TupleDesc tupdesc)
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

static int
get_row_width(HvaultPlanState *fdw_private)
{
    ListCell *c;
    int width = 0;
    foreach(c, fdw_private->coltypes)
    {
        switch(lfirst_int(c)) {
            case HvaultColumnFloatVal:
                width += sizeof(double);
                break;
            case HvaultColumnFileIdx:
            case HvaultColumnLineIdx:
            case HvaultColumnSampleIdx:
                width += 4;
                break;
            case HvaultColumnPoint:
                width += POINT_SIZE;
                break;
            case HvaultColumnFootprint:
                width += FOOTPRINT_SIZE;
                break;
            case HvaultColumnNull:
                /* nop */
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Undefined column type")));
        }
    }
    return width;
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

static List *
get_sort_pathkeys(PlannerInfo *root, RelOptInfo *baserel)
{
    List *pathkeys = NIL;
    ListCell *l, *m;
    PathKey *fileidx = NULL, *lineidx = NULL, *sampleidx = NULL;
    Oid opfamily;
    HvaultPlanState *fdw_private = (HvaultPlanState *) baserel->fdw_private;

    opfamily = get_opfamily_oid(BTREE_AM_OID, 
                                list_make1(makeString("integer_ops")), 
                                false);
    elog(DEBUG1, "processing pathkeys relid: %d opfam: %d", 
         baserel->relid, opfamily);
    foreach(l, root->eq_classes)
    {
        EquivalenceClass *ec = (EquivalenceClass *) lfirst(l);
        elog(DEBUG1, "processing EquivalenceClass");
        if (!bms_is_member(baserel->relid, ec->ec_relids))
        {
            elog(DEBUG1, "not my relid");
            continue;
        }

        foreach(m, ec->ec_members)
        {
            EquivalenceMember *em = (EquivalenceMember *) lfirst(m);
            elog(DEBUG1, "processing EquivalenceMember %s", 
                 nodeToString(em));
            if (IsA(em->em_expr, Var))
            {
                Var *var = (Var *) em->em_expr;
                if (baserel->relid == var->varno)
                {
                    PathKey *pathkey = NULL;
                    Assert(var->varattno < fdw_private->natts);
                    switch(list_nth_int(fdw_private->coltypes, var->varattno-1))
                    {
                        case HvaultColumnFileIdx:
                            if (!fileidx)
                            {
                                fileidx = pathkey = 
                                    (PathKey *) palloc(sizeof(PathKey));
                            } 
                            else 
                            {
                                elog(WARNING, "duplicate file index column");
                            }
                            break;
                        case HvaultColumnLineIdx:
                            if (!lineidx)
                            {
                                lineidx = pathkey = 
                                    (PathKey *) palloc(sizeof(PathKey));
                            } 
                            else 
                            {
                                elog(WARNING, "duplicate line index column");
                            }
                            break;
                        case HvaultColumnSampleIdx:
                            if (!sampleidx)
                            {
                                sampleidx = pathkey = 
                                    (PathKey *) palloc(sizeof(PathKey));
                            } 
                            else 
                            {
                                elog(WARNING, "duplicate sample index column");
                            }
                            break;
                        default:
                            /* nop */
                            break;
                    }

                    if (pathkey)
                    {
                        pathkey->pk_eclass = ec;
                        pathkey->pk_nulls_first = false;
                        pathkey->pk_strategy = BTLessStrategyNumber;
                        pathkey->pk_opfamily = opfamily;
                        elog(DEBUG1, "using this pathkey for sort");
                    }
                }
            }
        }
    }

    if (fileidx) 
    {
        pathkeys = lappend(pathkeys, fileidx);
        if (lineidx)
        {
            pathkeys = lappend(pathkeys, lineidx);
            if (sampleidx)
            {
                pathkeys = lappend(pathkeys, sampleidx);
            }
        }
        pathkeys = canonicalize_pathkeys(root, pathkeys);
        pathkeys = truncate_useless_pathkeys(root, baserel, pathkeys);
    }

    elog(DEBUG1, "pathkeys: %s", nodeToString(pathkeys));
    return pathkeys;
}

static HvaultSDSBuffer *
get_sds_buffer(List **buffers, char *name)
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

static size_t
hdf_sizeof(int32_t type)
{
    switch(type)
    {
        case DFNT_CHAR8:
        case DFNT_UCHAR8:
        case DFNT_INT8:
        case DFNT_UINT8:
            return 1;
        case DFNT_INT16:
        case DFNT_UINT16:
            return 2;
        case DFNT_INT32:
        case DFNT_UINT32:
        case DFNT_FLOAT32:
            return 4;
        case DFNT_INT64:
        case DFNT_UINT64:
        case DFNT_FLOAT64:
            return 8;
        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Unknown HDF datatype %d", type)));
            return -1;
    }
}

static double 
hdf_value(int32_t type, void *buffer, size_t offset)
{
    switch(type)
    {
        case DFNT_CHAR8:
            return ((signed char *)   buffer)[offset];
        case DFNT_UCHAR8:
            return ((unsigned char *) buffer)[offset];
        case DFNT_INT8:
            return ((int8_t *)        buffer)[offset];
        case DFNT_UINT8:
            return ((uint8_t *)       buffer)[offset];
        case DFNT_INT16:
            return ((int16_t *)       buffer)[offset];
        case DFNT_UINT16:
            return ((uint16_t *)      buffer)[offset];
        case DFNT_INT32:
            return ((int32_t *)       buffer)[offset];
        case DFNT_UINT32:
            return ((uint32_t *)      buffer)[offset];
        case DFNT_INT64:
            return ((int64_t *)       buffer)[offset];
        case DFNT_UINT64:
            return ((uint64_t *)      buffer)[offset];
        case DFNT_FLOAT32:
            return ((float *)         buffer)[offset];
        case DFNT_FLOAT64:
            return ((double *)        buffer)[offset];
        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Unknown HDF datatype %d", type)));
            return -1;
    }   
}

static bool
hdf_cmp(int32_t type, void *buffer, size_t offset, void *val)
{
    switch(type)
    {
        case DFNT_CHAR8:
            return ((signed char *)  buffer)[offset] == *((signed char *)  val);
        case DFNT_UCHAR8:
            return ((unsigned char *)buffer)[offset] == *((unsigned char *)val);
        case DFNT_INT8:
            return ((int8_t *)       buffer)[offset] == *((int8_t *)       val);
        case DFNT_UINT8:
            return ((uint8_t *)      buffer)[offset] == *((uint8_t *)      val);
        case DFNT_INT16:
            return ((int16_t *)      buffer)[offset] == *((int16_t *)      val);
        case DFNT_UINT16:
            return ((uint16_t *)     buffer)[offset] == *((uint16_t *)     val);
        case DFNT_INT32:
            return ((int32_t *)      buffer)[offset] == *((int32_t *)      val);
        case DFNT_UINT32:
            return ((uint32_t *)     buffer)[offset] == *((uint32_t *)     val);
        case DFNT_INT64:
            return ((int64_t *)      buffer)[offset] == *((int64_t *)      val);
        case DFNT_UINT64:
            return ((uint64_t *)     buffer)[offset] == *((uint64_t *)     val);
        case DFNT_FLOAT32:
            return ((float *)        buffer)[offset] == *((float *)        val);
        case DFNT_FLOAT64:
            return ((double *)       buffer)[offset] == *((double *)       val);
        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Unknown HDF datatype %d", type)));
            return -1;
    }   
}

/*
 * p1---p2
 * |     |
 * |  o  |
 * |     |
 * p3---p4
 */
static inline float 
interpolate_point(float p1, float p2, float p3, float p4)
{
    return ((float) 0.25) * (p1 + p2 + p3 + p4);
}


/*
 * p1----n1----?
 * |     |     |
 * |     |  o  |
 * |     |     |
 * p2----n2----?
 */
static inline float
extrapolate_point(float n1, float n2, float p1, float p2)
{
    return (3./4.) * (n1 + n2) - (1./4.) * (p1 + p2);
}

/*
 * lu----u-----?
 * |     |     |
 * |     |     |
 * |     |     |
 * l-----c-----?
 * |     |     |
 * |     |  o  |
 * |     |     |
 * ?-----?-----?
 */
static inline float 
extrapolate_corner_point(float c, float l, float u, float lu)
{
    return (9./4.) * c - (3./4.) * (l + u) + (1./4.) * lu;
}

/*
 * ?---p0---p1---p2---p3---     ---pm'---?
 * |    |    |    |    |    ...     |    | 
 * | r0 | r1 | r2 | r3 |    ...  rm'| rm | 
 * |    |    |    |    |    ...     |    | 
 * ?---n0---n1---n2---n3---     ---nm'---?
 */
static void
interpolate_line(size_t m, float const *p, float const *n, float *r)
{
	size_t i;
    r[0] = extrapolate_point(p[0], n[0], p[1], n[1]);
    r[m] = extrapolate_point(p[m-1], n[m-1], p[m-2], n[m-2]);
    for (i = 0; i < m-1; i++)
    {
        r[i+1] = interpolate_point(p[i], n[i], p[i+1], n[i+1]);
    }
}

/*
 * ?---p0---p1---p2---p3---     ---pm'---?
 * |    |    |    |    |    ...     |    | 
 * |    |    |    |    |    ...     |    | 
 * |    |    |    |    |    ...     |    | 
 * ?---n0---n1---n2---n3---     ---nm'---?
 * |    |    |    |    |    ...     |    | 
 * | r0 | r1 | r2 | r3 |    ...  rm'| rm | 
 * |    |    |    |    |    ...     |    | 
 * ?----?----?----?----?---     ----?----?
 */
static void
extrapolate_line(size_t m, float const *p, float const *n, float *r)
{
	size_t i;
    r[0] = extrapolate_corner_point(n[0], p[0], n[1], p[1]);
    r[m] = extrapolate_corner_point(n[m-1], p[m-1], n[m-2], p[m-2]);
    for (i = 0; i < m-1; i++)
    {
        r[i+1] = extrapolate_point(n[i], n[i+1], p[i], p[i+1]);
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
hdf_file_close(HvaultHDFFile *file)
{
    ListCell *l;

    Assert(file->sd_id != FAIL);
    foreach(l, file->sds)
    {
        HvaultSDSBuffer *sds = (HvaultSDSBuffer *) lfirst(l);
        if (sds->id == FAIL) continue;
        if (SDendaccess(sds->id) == FAIL)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), 
                            errmsg("Can't close SDS")));
        }
        sds->id = FAIL;
        sds->cur = sds->next = sds->prev = NULL;
        sds->fill_val = NULL;
    }
    if (SDend(file->sd_id) == FAIL)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't close HDF file")));
    }

    file->sd_id = FAIL;
    file->filename = NULL;
    MemoryContextReset(file->filememcxt);
    file->prevbrdlat = file->prevbrdlon = file->nextbrdlat = 
                       file->nextbrdlon = NULL;
    file->num_samples = -1;
    file->num_lines = -1;
}

static bool
hdf_file_open(HvaultExecState const *scan,
              char const *filename,
              HvaultHDFFile *file)
{
    ListCell *l;
    MemoryContext oldmemcxt;

    elog(DEBUG1, "loading hdf file %s", filename);
    Assert(file->filememcxt);
    oldmemcxt = MemoryContextSwitchTo(file->filememcxt);

    file->filename = filename;
    file->sd_id = SDstart(file->filename, DFACC_READ);
    if (file->sd_id == FAIL)
    {
        elog(WARNING, "Can't open HDF file %s, skipping file", file->filename);
        return false; 
    }
    
    foreach(l, file->sds)
    {
        HvaultSDSBuffer *sds = (HvaultSDSBuffer *) lfirst(l);
        int32_t sds_idx, rank, dims[H4_MAX_VAR_DIMS], sdnattrs, sdtype;
        double cal_err, offset_err;

        elog(DEBUG1, "Opening SDS %s", sds->name);

        /* Find sds */
        sds_idx = SDnametoindex(file->sd_id, sds->name);
        if (sds_idx == FAIL)
        {
            elog(WARNING, "Can't find dataset %s in file %s, skipping file",
                 sds->name, file->filename);
            MemoryContextSwitchTo(oldmemcxt);
            hdf_file_close(file);
            return false;
        }
        /* Select SDS */
        sds->id = SDselect(file->sd_id, sds_idx);
        if (sds->id == FAIL)
        {
            elog(WARNING, "Can't open dataset %s in file %s, skipping file",
                 sds->name, file->filename);
            MemoryContextSwitchTo(oldmemcxt);
            hdf_file_close(file);
            return false;
        }
        /* Get dimension sizes */
        if (SDgetinfo(sds->id, NULL, &rank, dims, &sds->type, &sdnattrs) == 
            FAIL)
        {
            elog(WARNING, "Can't get info about %s in file %s, skipping file",
                 sds->name, file->filename);
            MemoryContextSwitchTo(oldmemcxt);
            hdf_file_close(file);
            return false;
        }
        if (rank != 2)
        {
            elog(WARNING, "SDS %s in file %s has %dd dataset, skipping file",
                 sds->name, file->filename, rank);
            MemoryContextSwitchTo(oldmemcxt);
            hdf_file_close(file);
            return false;
        }
        if (file->num_lines == -1)
        {
            file->num_lines = dims[0];
            if (file->num_lines < 2 && scan->has_footprint)
            {
                elog(WARNING, 
                     "SDS %s in file %s has %d lines. Can't get footprint, skipping file",
                     sds->name, file->filename, file->num_lines);
                MemoryContextSwitchTo(oldmemcxt);
                hdf_file_close(file);
                return false;
            }
        } 
        else if (dims[0] != file->num_lines)
        {
            elog(WARNING, 
                 "SDS %s in file %s with %d lines is incompatible with others (%d), skipping file",
                 sds->name, file->filename, dims[0], file->num_lines);
            MemoryContextSwitchTo(oldmemcxt);
            hdf_file_close(file);
            return false;
        }
        if (file->num_samples == -1)
        {
            file->num_samples = dims[1];
        } 
        else if (dims[1] != file->num_samples)
        {
            elog(WARNING, 
                 "SDS %s in file %s with %d samples is incompatible with others (%d), skipping file",
                 sds->name, file->filename, dims[1], file->num_samples);
            MemoryContextSwitchTo(oldmemcxt);
            hdf_file_close(file);
            return false;
        }
        /* Get scale, offset & fill */
        sds->fill_val = palloc(hdf_sizeof(sds->type));
        if (SDgetfillvalue(sds->id, sds->fill_val) != SUCCEED)
        {
            pfree(sds->fill_val);
            sds->fill_val = NULL;
        }
        if (SDgetcal(sds->id, &sds->scale, &cal_err, &sds->offset, 
                     &offset_err, &sdtype) != SUCCEED)
        {
            sds->scale = 1.;
            sds->offset = 0;
        }
    }

    if (file->num_lines < 0 || file->num_samples < 0)
    {
        elog(WARNING, "Can't get number of lines and samples");
        MemoryContextSwitchTo(oldmemcxt);
        hdf_file_close(file);
        return false;
    }
    /* Allocate footprint buffers */
    /* TODO: check that allocation is OK */
    if (scan->has_footprint)
    {
        size_t bufsize = (file->num_samples + 1);
        file->prevbrdlat = (float *) palloc(sizeof(float) * bufsize);
        file->prevbrdlon = (float *) palloc(sizeof(float) * bufsize);
        file->nextbrdlat = (float *) palloc(sizeof(float) * bufsize);
        file->nextbrdlon = (float *) palloc(sizeof(float) * bufsize);
    }
    /* Allocate sd buffers */
    foreach(l, file->sds)
    {
        HvaultSDSBuffer *sds = (HvaultSDSBuffer *) lfirst(l);
        sds->cur = palloc(hdf_sizeof(sds->type) * file->num_samples);
        if (sds->haswindow)
        {
            sds->next = palloc(hdf_sizeof(sds->type) *
                               file->num_samples);
            sds->prev = palloc(hdf_sizeof(sds->type) *
                               file->num_samples);
        }
    }
    MemoryContextSwitchTo(oldmemcxt);
    return true;
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

static void
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
        scan->values[attnum] = Float8GetDatum(
            hdf_value(sds->type, sds->cur, scan->cur_sample) / sds->scale - 
            sds->offset);
    }
}

static void
fill_point_val(HvaultExecState const *scan, AttrNumber attnum)
{
    LWPOINT *point;
    LWGEOM *geom;
    GSERIALIZED *ret;
    point = lwpoint_make2d(SRID_UNKNOWN, 
        hdf_value(scan->lat->type, scan->lat->cur, scan->cur_sample),
        hdf_value(scan->lon->type, scan->lon->cur, scan->cur_sample));
    geom = lwpoint_as_lwgeom( point );
    ret = gserialized_from_lwgeom(geom, true, NULL);
    scan->nulls[attnum] = false;
    scan->values[attnum] = PointerGetDatum(ret);
}

static void
fill_footprint_val(HvaultExecState const *scan, AttrNumber attnum)
{
    LWPOLY *poly;
    LWGEOM *geom;
    GSERIALIZED *ret;
    POINTARRAY *points;
    POINT4D p = {0,0,0,0};

    points = ptarray_construct(false, false, 5);

    p.x = scan->file.prevbrdlat[scan->cur_sample];
    p.y = scan->file.prevbrdlon[scan->cur_sample];
    ptarray_set_point4d(points, 0, &p);

    p.x = scan->file.prevbrdlat[scan->cur_sample+1];
    p.y = scan->file.prevbrdlon[scan->cur_sample+1];
    ptarray_set_point4d(points, 1, &p);

    p.x = scan->file.nextbrdlat[scan->cur_sample+1];
    p.y = scan->file.nextbrdlon[scan->cur_sample+1];
    ptarray_set_point4d(points, 2, &p);

    p.x = scan->file.nextbrdlat[scan->cur_sample];
    p.y = scan->file.nextbrdlon[scan->cur_sample];
    ptarray_set_point4d(points, 3, &p);

    p.x = scan->file.prevbrdlat[scan->cur_sample];
    p.y = scan->file.prevbrdlon[scan->cur_sample];
    ptarray_set_point4d(points, 4, &p);

    poly = lwpoly_construct(SRID_UNKNOWN, NULL, 1, &points);
    geom = lwpoly_as_lwgeom(poly);
    ret = gserialized_from_lwgeom(geom, true, NULL);
    scan->nulls[attnum] = false;
    scan->values[attnum] = PointerGetDatum(ret);   
}

static double 
get_num_files(HvaultPlanState *fdw_private)
{
    StringInfo query_str;
    Datum val;
    int64_t num_files;
    bool isnull;

    if (!fdw_private->catalog)
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
    appendStringInfo(query_str, "SELECT COUNT(*) FROM %s", 
                     fdw_private->catalog);
    if (SPI_execute(query_str->data, true, 1) != SPI_OK_SELECT || 
        SPI_processed != 1)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't get number of rows in catalog %s", 
                               fdw_private->catalog)));
        return 0; /* Will never reach this */      
    }
    pfree(query_str->data);
    pfree(query_str);
    if (SPI_tuptable->tupdesc->natts != 1 ||
        SPI_tuptable->tupdesc->attrs[0]->atttypid != INT8OID)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't get number of rows in catalog %s", 
                               fdw_private->catalog)));
        return 0; /* Will never reach this */         
    }
    val = heap_getattr(SPI_tuptable->vals[0], 1, 
                       SPI_tuptable->tupdesc, &isnull);
    if (isnull)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't get number of rows in catalog %s", 
                               fdw_private->catalog)));
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

static bool
fetch_next_file(HvaultExecState *scan)
{
    Portal file_cursor;
    if (scan->file_cursor_name == NULL)
    {
        /* Initialize cursor */
        StringInfo query_str;
        SPIPlanPtr prepared_stmt;
            
        Assert(scan->catalog);
        if (SPI_connect() != SPI_OK_CONNECT)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't connect to SPI")));
            return false; /* Will never reach this */
        }

        query_str = makeStringInfo();
        appendStringInfo(query_str, 
                         "SELECT file_id, filename, filetime FROM %s",
                         scan->catalog);
        prepared_stmt = SPI_prepare(query_str->data, 0, NULL);
        if (!prepared_stmt)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't prepare query for catalog %s", 
                                   scan->catalog)));
            return false; /* Will never reach this */
        }
        file_cursor = SPI_cursor_open(NULL, prepared_stmt, NULL, NULL, true);
        scan->file_cursor_name = file_cursor->name;


        if (SPI_finish() != SPI_OK_FINISH)    
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't finish access to SPI")));
            return false; /* Will never reach this */   
        }
    } 
    else 
    {
        /* Close previous file */
        hdf_file_close(&scan->file);
    }
    
    file_cursor = SPI_cursor_find(scan->file_cursor_name);
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
            return false;
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
        scan->file_time = DatumGetTimestamp(val);

        hdf_file_open(scan, filename, &scan->file);
    }
    while(scan->file.sd_id == -1);
    return true;
}
