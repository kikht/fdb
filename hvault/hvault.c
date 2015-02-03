#include "common.h"

#define int8 hdf_int8
#include <hdf/mfhdf.h>
#undef int8

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
 
extern void             hvaultExplain    (ForeignScanState * node, 
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

#if LIBLWGEOM_VERSION_MAJOR_INT >= 2 && LIBLWGEOM_VERSION_MINOR_INT >= 1
#define ERRMSG_MAXLEN 256

static void
pg_error(const char *fmt, va_list ap)
{
    char errmsg[ERRMSG_MAXLEN+1];

    vsnprintf (errmsg, ERRMSG_MAXLEN, fmt, ap);

    errmsg[ERRMSG_MAXLEN]='\0';
    ereport(ERROR, (errmsg_internal("%s", errmsg)));
}

static void
pg_notice(const char *fmt, va_list ap)
{
    char errmsg[ERRMSG_MAXLEN+1];

    vsnprintf (errmsg, ERRMSG_MAXLEN, fmt, ap);

    errmsg[ERRMSG_MAXLEN]='\0';
    ereport(NOTICE, (errmsg_internal("%s", errmsg)));
}

static void 
init_lwgeom_handlers()
{
    lwgeom_set_handlers(palloc, repalloc, pfree, pg_error, pg_notice); 
}

#else

static void 
init_lwgeom_handlers()
{
}

#endif

void
_PG_init(void)
{
    init_lwgeom_handlers();
    HDdont_atexit();
    GDALAllRegister();
}

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
