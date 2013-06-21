#include "common.h"

extern void             hvaultGetRelSize (PlannerInfo * root, 
                                          RelOptInfo *  baserel, 
                                          Oid           foreigntableid);

extern void             hvaultGetPaths   (PlannerInfo * root, 
                                          RelOptInfo *  baserel,
                                          Oid           foreigntableid);
 
extern ForeignScan *    hvaultGetPlan    (PlannerInfo * root, 
                                          RelOptInfo *  baserel,
                                          Oid           foreigntableid, 
                                          ForeignPath * best_path,
                                          List *        tlist, 
                                          List *        scan_clauses);
 
 extern void            hvaultExplain    (ForeignScanState * node, 
                                          ExplainState *     es);
 
extern void             hvaultBegin      (ForeignScanState *node, int eflags);
extern TupleTableSlot * hvaultIterate    (ForeignScanState *node);
extern void             hvaultReScan     (ForeignScanState *node);
extern void             hvaultEnd        (ForeignScanState *node);
extern bool             hvaultAnalyze    (Relation                relation, 
                                          AcquireSampleRowsFunc * func,
                                          BlockNumber *           totalpages);

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

const int hvaultDatatypeSize[HvaultNumDatatypes] = 
    {1, 1, 2, 2, 4, 4, 8, 8, 4, 8, -1};

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

#ifdef USE_ASSERT_CHECKING
    assert_enabled = true;
#endif

    PG_RETURN_POINTER(fdwroutine);
}

PG_FUNCTION_INFO_V1(hvault_fdw_validator);
Datum 
hvault_fdw_validator(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(true);
}
