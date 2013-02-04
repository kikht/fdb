#include <postgres.h>
#include <funcapi.h>
#include <access/skey.h>
#include <commands/defrem.h>
#include <commands/explain.h>
#include <foreign/fdwapi.h>
#include <nodes/bitmapset.h>
#include <optimizer/planmain.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/restrictinfo.h>
#include <utils/rel.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct HvaultPlanState
{
} HvaultPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct HvaultExecState
{
    int row_num;
    bool * nulls;
    Datum * values;
} HvaultExecState;


/*
 * SQL functions
 */
extern Datum hvault_fdw_validator(PG_FUNCTION_ARGS);
extern Datum hvault_fdw_handler(PG_FUNCTION_ARGS);

/* 
 * FDW callback routines 
 */
static void 
hvaultGetRelSize(PlannerInfo *root, 
                 RelOptInfo *baserel, 
                 Oid foreigntableid);
static void 
hvaultGetPaths(PlannerInfo *root, 
               RelOptInfo *baserel,
               Oid foreigntableid);
static ForeignScan *
hvaultGetPlan(PlannerInfo *root, 
              RelOptInfo *baserel,
              Oid foreigntableid, 
              ForeignPath *best_path,
              List *tlist, 
              List *scan_clauses);
/*
static bool 
hvaultAnalyze(Relation relation, 
              AcquireSampleRowsFunc *func,
              BlockNumber *totalpages );
*/
static void hvaultExplain(ForeignScanState *node, ExplainState *es);
static void hvaultBegin(ForeignScanState *node, int eflags);
static TupleTableSlot *hvaultIterate(ForeignScanState *node);
static void hvaultReScan(ForeignScanState *node);
static void hvaultEnd(ForeignScanState *node);




/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
PG_FUNCTION_INFO_V1(hvault_fdw_handler);
Datum 
hvault_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine * fdwroutine = makeNode(FdwRoutine);

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

    fdw_private = (HvaultPlanState *) palloc(sizeof(HvaultPlanState));

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
      
    /* estimated number of tuples in the relation after 
       restriction clauses have been applied */
    baserel->rows = 10000; 
    /* avg. number of bytes per tuple in the relation after the
       appropriate projections have been done */
    baserel->width = sizeof(Datum);
    baserel->fdw_private = fdw_private;

    elog(DEBUG1, "GetRelSize: baserestrictinfo: %s joininfo: %s",
         nodeToString(baserel->baserestrictinfo),
         nodeToString(baserel->joininfo));
}

static void 
hvaultGetPaths(PlannerInfo *root, 
               RelOptInfo *baserel,
               Oid foreigntableid)
{
    ForeignPath *path = NULL;
    double rows;           /* estimate number of rows returned by path */
    Cost startup_cost;     /* cost expended before fetching any tuples */
    Cost total_cost;       /* total cost (assuming all tuples fetched) */                     
    List *pathkeys = NIL;  /* represent a pre-sorted result */
    Relids required_outer = NULL; /* Bitmap of rels supplying parameters used 
                                     by path. Look at the baserel.joininfo and 
                                     equivalence classes to generate possible 
                                     parametrized paths*/
    List *fdw_path_private = NIL;/* best practice is to use a representation 
                               that's dumpable by nodeToString, for use with 
                               debugging support available in the backend. 
                               List of DefElem suits well for this*/
    HvaultPlanState *fdw_private = (HvaultPlanState *) baserel->fdw_private;
    ListCell *l, *m;
    Index sortcolidx = 1;
    PathKey *sortcol = NULL;
    Oid opfamily;
    /* baserel->fdw_private - available from GetRelSize */

    /*
    bool
    add_path_precheck(RelOptInfo *parent_rel,
                      Cost startup_cost, Cost total_cost,
                      List *pathkeys, Relids required_outer)
    Can be useful to reject uninteresting paths before creating path struct
    */ 
    
    rows = 10000;
    startup_cost = 10;
    total_cost = 2000;

    if (has_useful_pathkeys(root, baserel))
    {
        opfamily = get_opfamily_oid(BTREE_AM_OID, 
                                    list_make1(makeString("integer_ops")), 
                                    false);
        elog(DEBUG1, "processing pathkeys sortcolidx: %d relid: %d opfam: %d", 
             sortcolidx, baserel->relid, opfamily);
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
                    Var * var = (Var *) em->em_expr;
                    if (baserel->relid == var->varno && 
                        var->varattno == sortcolidx)
                    {
                        sortcol = (PathKey *) palloc(sizeof(PathKey));
                        sortcol->pk_eclass = ec;
                        sortcol->pk_nulls_first = false;
                        sortcol->pk_strategy = BTLessStrategyNumber;
                        sortcol->pk_opfamily = opfamily;

                        elog(DEBUG1, "using this pathkey for sort");
                        break;
                    }
                }
            }

            if (sortcol != NULL) break;
        }
    }
    if (sortcol != NULL)
    {
        pathkeys = lappend(pathkeys, sortcol);
        pathkeys = canonicalize_pathkeys(root, pathkeys);
        pathkeys = truncate_useless_pathkeys(root, baserel, pathkeys);
    }

    elog(DEBUG1, "pathkeys: %s", nodeToString(pathkeys));

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
    Oid foreigntableoid = RelationGetRelid(node->ss.ss_currentRelation);
    ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
    List * fdw_plan_private = plan->fdw_private;
    List * fdw_exprs = plan->fdw_exprs;

    elog(DEBUG1, "in hvaultBegin");

    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    fdw_exec_state = (HvaultExecState *) palloc(sizeof(HvaultExecState));
    fdw_exec_state->row_num = 0;
    fdw_exec_state->nulls = palloc(sizeof(bool) * 1);
    fdw_exec_state->values = palloc(sizeof(Datum) * 1);

    node->fdw_state = fdw_exec_state;
}

static TupleTableSlot *
hvaultIterate(ForeignScanState *node) 
{
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    HvaultExecState *fdw_exec_state = (HvaultExecState *) node->fdw_state;

    elog(DEBUG3, "in hvaultIterate %d", fdw_exec_state->row_num);

    if (fdw_exec_state->row_num == 0)
    {
        elog(DEBUG1, "first hvaultIterate");
        
    }

    Assert(tupdesc->natts == 1);
    ExecClearTuple(slot);
    if (fdw_exec_state->row_num < 10000)
    {
        slot->tts_values = fdw_exec_state->values;
        slot->tts_isnull = fdw_exec_state->nulls;
        slot->tts_values[0] = false;
        slot->tts_values[0] = fdw_exec_state->row_num;
        ExecStoreVirtualTuple(slot);

        fdw_exec_state->row_num++;
    } 

    return slot;
}

static void 
hvaultReScan(ForeignScanState *node)
{
     HvaultExecState *fdw_exec_state = (HvaultExecState *) node->fdw_state;
     fdw_exec_state->row_num = 0;
     elog(DEBUG1, "in hvaultReScan");
}


static void 
hvaultEnd(ForeignScanState *node)
{
    HvaultExecState *fdw_exec_state = (HvaultExecState *) node->fdw_state;

    elog(DEBUG1, "in hvaultEnd");
    if (fdw_exec_state != NULL)
    {
        pfree(fdw_exec_state->nulls);
        pfree(fdw_exec_state->values);
        pfree(fdw_exec_state);
    }
}
