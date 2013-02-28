#include <postgres.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <foreign/foreign.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pg_list.h>
#include <nodes/primnodes.h>
#include <nodes/relation.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/planmain.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/syscache.h>

#include "hvault.h"

/* 
 * This file includes routines involved in query planning
 */

static HvaultColumnType *get_column_types(PlannerInfo *root, 
                                          RelOptInfo *baserel, 
                                          Oid foreigntableid,
                                          AttrNumber natts);
static double get_num_files(char *catalog);
static int    get_row_width(HvaultColumnType *coltypes, AttrNumber numattrs);
static List  *get_sort_pathkeys(PlannerInfo *root, RelOptInfo *baserel);
static bool   bms_equal_any(Relids relids, List *relids_list);
static void   extractCatalogQuals(PlannerInfo *root, 
                                  RelOptInfo *baserel,
                                  HvaultTableInfo const *table,
                                  List **catalog_quals,
                                  List **catalog_joins,
                                  List **catalog_ec);
static void   deparseExpr(Expr *node, HvaultDeparseContext *ctx);


/* 
 * This function is intened to provide first estimate of a size of relation
 * involved in query. Generally we have all query information available at 
 * planner stage and can build our estimate basing on:
 *  baserel->reltargetlist - List of Var and PlaceHolderVar nodes for 
 *      the values we need to output from this relation. 
 *  baserel->baserestrictinfo - 
 *      List of RestrictInfo nodes, containing info about
 *      each non-join qualification clause in which this relation
 *      participates 
 *  baserel->joininfo - 
 *      List of RestrictInfo nodes, containing info about each
 *      join clause in which this relation participates 
 *  root->eq_classes - 
 *      List of EquivalenceClass nodes that were extracted from query.
 *      These lists are transitive closures of of the expressions like a = b
 *
 * Currently our estimate is quite simple:
 * rows = (total number of files in catalog) * (max tuples per file)
 * 
 * To get row width we detect unused columns here and sum the size of others
 * (we actually know the size of every column we can emit)
 *
 * TODO: we should extract catalog quals to get better estimate of number
 *       of files involved in query.
 */
void 
hvaultGetRelSize(PlannerInfo *root, 
                 RelOptInfo *baserel, 
                 Oid foreigntableid)
{
    HvaultTableInfo *fdw_private;
    double num_files, tuples_per_file, scale_factor;
    Relation rel;
    TupleDesc tupleDesc;

    fdw_private = (HvaultTableInfo *) palloc(sizeof(HvaultTableInfo));

    rel = heap_open(foreigntableid, AccessShareLock);
    tupleDesc = RelationGetDescr(rel);
    fdw_private->natts = tupleDesc->natts;
    heap_close(rel, AccessShareLock);    
    
    fdw_private->relid = baserel->relid;
    fdw_private->coltypes = get_column_types(root, baserel, foreigntableid, 
                                             fdw_private->natts);
    fdw_private->catalog = get_table_option(foreigntableid, "catalog");

    // TODO: Use constant catalog quals for better estimate
    num_files = get_num_files(fdw_private->catalog);
    tuples_per_file = TUPLES_PER_FILE;
    scale_factor = 1; /* 4 for 500m, 16 for 250m */

    baserel->width = get_row_width(fdw_private->coltypes, fdw_private->natts);
    baserel->rows = num_files * tuples_per_file * scale_factor ; 
    baserel->fdw_private = fdw_private;

    elog(DEBUG2, "GetRelSize: baserestrictinfo: %s\njoininfo: %s",
         nodeToString(baserel->baserestrictinfo),
         nodeToString(baserel->joininfo));
}

void 
hvaultGetPaths(PlannerInfo *root, 
               RelOptInfo *baserel,
               Oid foreigntableid)
{
    HvaultTableInfo *fdw_private = (HvaultTableInfo *) baserel->fdw_private;
    ForeignPath *path = NULL;
    List *pathkeys = NIL;  /* represent a pre-sorted result */

    HvaultPathData *plain_path;
    ListCell *l, *m, *k;
    List *catalog_quals = NIL;
    List *catalog_joins = NIL;
    List *catalog_ec = NIL;
    List *considered_relids = NIL;

    /* Process pathkeys */
    if (has_useful_pathkeys(root, baserel))
    {
        /* TODO: time sort */
        pathkeys = get_sort_pathkeys(root, baserel);
    }

    extractCatalogQuals(root, baserel, fdw_private, 
                        &catalog_quals, &catalog_joins, &catalog_ec);
    
    /* Create simple unparametrized path */
    plain_path = palloc(sizeof(HvaultPathData));
    plain_path->table = fdw_private;
    plain_path->catalog_quals = catalog_quals;
    // TODO: use catalog quals to better estimate number of rows and costs 
    path = create_foreignscan_path(root, 
                                   baserel, 
                                   baserel->rows, 
                                   10,                     /* startup cost */
                                   baserel->rows * 0.001,  /* total cost */
                                   pathkeys, 
                                   NULL,                   /* required outer */
                                   (List *) plain_path);
    add_path(baserel, (Path *) path);
    
    /* Create parametrized join paths */
    // foreach(l, catalog_joins)
    // {
    //     RestrictInfo *rinfo = lfirst(l);
    //     Relids clause_relids = rinfo->clause_relids;
    //     if (bms_equal_any(clause_relids, considered_relids))
    //         continue;

    //     foreach(m, considered_relids)
    //     {
    //         Relids oldrelids = (Relids) lfirst(m);

    //         /*
    //          * If either is a subset of the other, no new set is possible.
    //          * This isn't a complete test for redundancy, but it's easy and
    //          * cheap.  get_join_index_paths will check more carefully if we
    //          * already generated the same relids set.
    //          */
    //         if (bms_subset_compare(clause_relids, oldrelids) != BMS_DIFFERENT)
    //             continue;

    //         // /*
    //         //  * If this clause was derived from an equivalence class, the
    //         //  * clause list may contain other clauses derived from the same
    //         //  * eclass.  We should not consider that combining this clause with
    //         //  * one of those clauses generates a usefully different
    //         //  * parameterization; so skip if any clause derived from the same
    //         //  * eclass would already have been included when using oldrelids.
    //         //  */
    //         // if (rinfo->parent_ec && ecAlreadyUsed(rinfo->parent_ec, oldrelids,
    //         //                                       catalog_join_quals, 
    //         //                                       catalog_ec)))
    //         // {
                
    //         //     continue;
    //         // }
                

    //         // /*
    //         //  * If the number of relid sets considered exceeds our heuristic
    //         //  * limit, stop considering combinations of clauses.  We'll still
    //         //  * consider the current clause alone, though (below this loop).
    //         //  */
    //         // if (list_length(*considered_relids) >= 10 * considered_clauses)
    //         //     break;       

    //         //TODO: consider union relid
    //     }

    //     //TODO: consider relids by itself
    // }
}

ForeignScan *
hvaultGetPlan(PlannerInfo *root, 
              RelOptInfo *baserel,
              Oid foreigntableid, 
              ForeignPath *best_path,
              List *tlist, 
              List *scan_clauses)
{
    HvaultPathData *fdw_private = (HvaultPathData *) best_path->fdw_private; 

    List *rest_clauses = NIL; /* clauses that must be checked externally */
    List *fdw_plan_private = NIL;
    ListCell *l;
    HvaultDeparseContext deparse_ctx;
    bool first_qual;
    List *coltypes;
    AttrNumber attnum;

    /* Prepare catalog query */
    deparse_ctx.table = fdw_private->table;
    deparse_ctx.fdw_expr = NIL;
    initStringInfo(&deparse_ctx.query);
    first_qual = true;
    foreach(l, fdw_private->catalog_quals)
    {   
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
        if (first_qual)
        {
            appendStringInfoString(&deparse_ctx.query, "WHERE ");
        }
        else 
        {
            appendStringInfoString(&deparse_ctx.query, " AND ");
        }
        deparseExpr(rinfo->clause, &deparse_ctx);
    }

    /* Extract clauses that need to be checked externally */
    foreach(l, scan_clauses)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
        if (!list_member_ptr(fdw_private->catalog_quals, rinfo))
        {
            rest_clauses = lappend(rest_clauses, rinfo);
        }
        else
        {
            elog(DEBUG1, "Skipping catalog clause %s", 
                 nodeToString(rinfo->clause));
        }
    }
    rest_clauses = extract_actual_clauses(scan_clauses, false);

    elog(DEBUG2, "GetPlan: scan_cl: %s\nrest_cl: %s",
         nodeToString(scan_clauses),
         nodeToString(rest_clauses));
    elog(DEBUG3, "GetPlan: tlist: %s", nodeToString(tlist));

    /* store fdw_private in List */
    coltypes = NIL;
    for (attnum = 0; attnum < fdw_private->table->natts; attnum++)
    {
        coltypes = lappend_int(coltypes, fdw_private->table->coltypes[attnum]);
    }
    fdw_plan_private = lappend(fdw_plan_private, 
                               makeString(deparse_ctx.query.data));
    fdw_plan_private = lappend(fdw_plan_private, coltypes);

    return make_foreignscan(tlist, rest_clauses, baserel->relid, 
                            deparse_ctx.fdw_expr, fdw_plan_private);
}

typedef struct 
{
    HvaultColumnType *types;
    Index relid;
    Oid foreigntableid;
    AttrNumber natts;
} HvaultColumnTypeWalkerContext;

static bool 
get_column_types_walker(Node *node, HvaultColumnTypeWalkerContext *cxt)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var))
    {
        Var *var = (Var *) node;
        if (var->varno == cxt->relid)
        {
            AttrNumber attnum;
            Assert(var->varattno <= natts);
            attnum = var->varattno-1;
            if (cxt->types[attnum] == HvaultColumnNull)
            {
                List *colopts;
                ListCell *m;

                cxt->types[attnum] = HvaultColumnFloatVal;
                colopts = GetForeignColumnOptions(cxt->foreigntableid, 
                                                  var->varattno);
                foreach(m, colopts)
                {
                    DefElem *opt = (DefElem *) lfirst(m);
                    if (strcmp(opt->defname, "type") == 0)
                    {
                        char *type = defGetString(opt);

                        if (strcmp(type, "point") == 0) 
                        {
                            cxt->types[attnum] = HvaultColumnPoint;
                        }
                        else if (strcmp(type, "footprint") == 0)
                        {
                            cxt->types[attnum] = HvaultColumnFootprint;   
                        }
                        else if (strcmp(type, "file_index") == 0)
                        {
                            cxt->types[attnum] = HvaultColumnFileIdx;
                        }
                        else if (strcmp(type, "line_index") == 0)
                        {
                            cxt->types[attnum] = HvaultColumnLineIdx;
                        }
                        else if (strcmp(type, "sample_index") == 0)
                        {
                            cxt->types[attnum] = HvaultColumnSampleIdx;
                        }
                        else if (strcmp(type, "time") == 0)
                        {
                            cxt->types[attnum] = HvaultColumnTime;
                        }
                        else
                        {
                            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                                            errmsg("Unknown column type %s", 
                                                   type)));
                        }
                        elog(DEBUG1, "col: %d strtype: %s type: %d", 
                             attnum, type, cxt->types[attnum]);
                    }
                }
            }
        }
    }
    return expression_tree_walker(node, get_column_types_walker, (void *) cxt);
}

static HvaultColumnType *
get_column_types(PlannerInfo *root, 
                 RelOptInfo *baserel, 
                 Oid foreigntableid,
                 AttrNumber natts)
{
    ListCell *l, *m;
    AttrNumber attnum;
    HvaultColumnTypeWalkerContext walker_cxt;

    walker_cxt.natts = natts;
    walker_cxt.relid = baserel->relid;
    walker_cxt.foreigntableid = foreigntableid;
    walker_cxt.types = palloc(sizeof(HvaultColumnType) * walker_cxt.natts);
    for (attnum = 0; attnum < walker_cxt.natts; attnum++)
    {
        walker_cxt.types[attnum] = HvaultColumnNull;
    }

    get_column_types_walker((Node *) baserel->reltargetlist, &walker_cxt);
    foreach(l, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
        get_column_types_walker((Node *) rinfo->clause, &walker_cxt);
    }

    foreach(l, baserel->joininfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
        get_column_types_walker((Node *) rinfo->clause, &walker_cxt);
    }

    foreach(l, root->eq_classes)
    {
        EquivalenceClass *ec = (EquivalenceClass *) lfirst(l);
        if (!bms_is_member(baserel->relid, ec->ec_relids))
        {
            continue;
        }
        foreach(m, ec->ec_members)
        {
            EquivalenceMember *em = (EquivalenceMember *) lfirst(m);
            get_column_types_walker((Node *) em->em_expr, &walker_cxt);
        }
    }

    // for (attnum = 0; attnum < walker_cxt.natts; attnum++)
    // {
    //     res = lappend_int(res, walker_cxt.types[attnum]);
    // }
    // pfree(walker_cxt.types);
    return walker_cxt.types;
}

static int
get_row_width(HvaultColumnType *coltypes, AttrNumber numattrs)
{
    int width = 0;
    AttrNumber i;

    for (i = 0; i < numattrs; ++i)
    {
        switch (coltypes[i]) {
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
            case HvaultColumnTime:
                width += 8;
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

static double 
get_num_files(char *catalog)
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

static List *
get_sort_pathkeys(PlannerInfo *root, RelOptInfo *baserel)
{
    List *pathkeys = NIL;
    ListCell *l, *m;
    PathKey *fileidx = NULL, *lineidx = NULL, *sampleidx = NULL;
    Oid opfamily;
    HvaultTableInfo *fdw_private = (HvaultTableInfo *) baserel->fdw_private;

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
                    switch(fdw_private->coltypes[var->varattno-1])
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

    elog(DEBUG2, "pathkeys: %s", nodeToString(pathkeys));
    return pathkeys;
}

static inline bool 
isCatalogVar(HvaultColumnType type)
{
    return type == HvaultColumnFileIdx || type == HvaultColumnTime;
}

typedef struct
{
    HvaultTableInfo const table;
} CatalogQualsContext;

static bool 
isCatalogQualWalker(Node *node, CatalogQualsContext *ctx)
{
    if (node == NULL)
        return false;

    switch (nodeTag(node))
    {
        case T_Var:
            {
                /*
                 * If Var is in our table, then check its type and correct 
                 * condition type if necessary.
                 * Ignore other tables, because this function can also be used 
                 * with join conditions.
                 */
                Var *var = (Var *) node;
                if (var->varno == ctx->table.relid)
                {
                    if (!isCatalogVar(ctx->table.coltypes[var->varattno-1]))
                        return true;
                }
            }
            break;
        case T_Param:
        case T_ArrayRef:
        case T_FuncExpr:
        case T_Const:
        case T_OpExpr:
        case T_DistinctExpr:
        case T_ScalarArrayOpExpr:
        case T_RelabelType:
        case T_BoolExpr:
        case T_NullTest:
        case T_ArrayExpr:
        case T_List:
            /* OK */
            break;
        default:
            /*
             * If it's anything else, assume it's unsafe.  This list can be
             * expanded later, but don't forget to add deparse support below.
             */
            return true;
    }

    /* Recurse to examine sub-nodes */
    return expression_tree_walker(node, isCatalogQualWalker, (void *) ctx);
}

/* Little trick to ensure that table info is const */
static inline bool 
isCatalogQual(Expr *expr, HvaultTableInfo const *table)
{
    return !isCatalogQualWalker((Node *) expr, (CatalogQualsContext *) &table);
}

/* Catalog only EC will be put into baserestrictinfo by planner, so here
 * we need to extract only ECs that contain both catalog & outer table vars
 */
static bool
isCatalogJoinEC(EquivalenceClass *ec, HvaultTableInfo const *table)
{
    ListCell *l;
    int num_catalog_vars = 0;
    int num_outer_vars = 0;

    foreach(l, ec->ec_members)
    {
        EquivalenceMember *em = (EquivalenceMember *) lfirst(l);
        if (IsA(em->em_expr, Var))
        {
            Var *var = (Var *) em->em_expr;
            if (var->varno == table->relid)
            {
                if (isCatalogVar(table->coltypes[var->varattno-1]))
                {
                    num_catalog_vars++;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                num_outer_vars++;
            }
        }
        else
        {
            /* Should be Const here, but check it in more general way */
            if (!isCatalogQual(em->em_expr, table))
                return 0;
        }
    }
    return num_catalog_vars > 0 && num_outer_vars > 0;
}

static void
extractCatalogQuals(PlannerInfo *root, 
                    RelOptInfo *baserel,
                    HvaultTableInfo const *table,
                    List **catalog_quals,
                    List **catalog_joins,
                    List **catalog_ec)
{
    ListCell *l;

    foreach(l, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = lfirst(l);
        if (isCatalogQual(rinfo->clause, table))
        {
            *catalog_quals = lappend(*catalog_quals, rinfo);
        } 
    }

    foreach(l, baserel->joininfo)
    {
        RestrictInfo *rinfo = lfirst(l);
        if (isCatalogQual(rinfo->clause, table))
        {
            *catalog_joins = lappend(*catalog_joins, rinfo);
        }
    }

    foreach(l, root->eq_classes)
    {
        EquivalenceClass *ec = (EquivalenceClass *) lfirst(l);
        if (isCatalogJoinEC(ec, table) == 1)
        {
            *catalog_ec = lappend(*catalog_ec, ec);
        }
    }
}

static bool 
bms_equal_any(Relids relids, List *relids_list)
{
    ListCell   *lc;

    foreach(lc, relids_list)
    {
        if (bms_equal(relids, (Relids) lfirst(lc)))
            return true;
    }
    return false;
}



/* -----------------------------------
 *
 * Deparse expression functions
 *
 * These functions examine query clauses and translate them to corresponding 
 * catalog query clauses. It is intended to reduce number of files we need to 
 * scan for the query. Lots of deparse functions are ported from 
 * contrib/postgres_fdw/deparse.c and ruleutils.c. It seems that deparsing 
 * API should be extended somehow to handle such cases.
 *
 * -----------------------------------
 */


/* 
 * Deparse expression as runtime computable parameter. 
 * Parameter index directly maps to fdw_expr.
 */
static void 
deparseParameter(Expr *node, HvaultDeparseContext *ctx)
{
    ListCell *l;
    int idx = 0;

    foreach(l, ctx->fdw_expr)
    {
        if (lfirst(l) == node)
            break;
        idx++;
    }
    ctx->fdw_expr = lappend(ctx->fdw_expr, node);
    appendStringInfo(&ctx->query, "$%d", idx + 1);
}

/*
 * Deparse variable expression. 
 * Local file_id and time variables are mapped to corresponding catalog column.
 * Other local variables are not supported. Footprint variables are processed 
 * separately. Other's table variables are handled as runtime computable 
 * parameters, this is the case of parametrized join path.
 */
static void
deparseVar(Var *node, HvaultDeparseContext *ctx)
{
    if (node->varno == ctx->table->relid)
    {
        /* Catalog column */
        HvaultColumnType type;
        char *colname = NULL;
        
        Assert(node->varattno > 0);
        Assert(node->varattno <= ctx->natts);

        type = ctx->table->coltypes[node->varattno-1];
        switch(type)
        {
            case HvaultColumnTime:
                colname = "starttime";
                
            break;
            case HvaultColumnFileIdx:
                colname = "file_id";
            break;
            default:
                elog(ERROR, "unsupported local column type: %d", type);
        }
        appendStringInfoString(&ctx->query, quote_identifier(colname));
    }
    else
    {
        /* Other table's column, use as parameter */
        deparseParameter((Expr *) node, ctx);
    }
}

static void
deparseFuncExpr(FuncExpr *node, HvaultDeparseContext *ctx)
{
    HeapTuple proctup;
    Form_pg_proc procform;
    const char *proname;
    bool first;
    ListCell *arg;

    /*
     * If the function call came from an implicit coercion, then just show the
     * first argument.
     */
    if (node->funcformat == COERCE_IMPLICIT_CAST)
    {
        deparseExpr((Expr *) linitial(node->args), ctx);
        return;
    }

    /*
     * If the function call came from a cast, then show the first argument
     * plus an explicit cast operation.
     */
    if (node->funcformat == COERCE_EXPLICIT_CAST)
    {
        Oid rettype = node->funcresulttype;
        int32_t coercedTypmod;

        /* Get the typmod if this is a length-coercion function */
        (void) exprIsLengthCoercion((Node *) node, &coercedTypmod);

        deparseExpr((Expr *) linitial(node->args), ctx);
        appendStringInfo(&ctx->query, "::%s",
                         format_type_with_typemod(rettype, coercedTypmod));
        return;
    }

    /*
     * Normal function: display as proname(args).
     */
    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(node->funcid));
    if (!HeapTupleIsValid(proctup)) 
    {
        elog(ERROR, "cache lookup failed for function %u", node->funcid);
        return; /* Will never reach here */    
    }
    procform = (Form_pg_proc) GETSTRUCT(proctup);

    
    if (OidIsValid(procform->provariadic))
    {
        elog(ERROR, "Variadic functions are not supported");
    }

    /* Print schema name only if it's not pg_catalog */
    if (procform->pronamespace != PG_CATALOG_NAMESPACE)
    {
        const char *schemaname;
        schemaname = get_namespace_name(procform->pronamespace);
        schemaname = quote_identifier(schemaname);
        appendStringInfo(&ctx->query, "%s.", schemaname);
    }

    /* Deparse the function name ... */
    proname = NameStr(procform->proname);
    appendStringInfo(&ctx->query, "%s(", quote_identifier(proname));
    /* ... and all the arguments */
    first = true;
    foreach(arg, node->args)
    {
        if (!first)
            appendStringInfoString(&ctx->query, ", ");
        deparseExpr((Expr *) lfirst(arg), ctx);
        first = false;
    }
    appendStringInfoChar(&ctx->query, ')');

    ReleaseSysCache(proctup);
}

static void
deparseOperatorName(StringInfo buf, Form_pg_operator opform)
{
    /* opname is not a SQL identifier, so we should not quote it. */
    char *opname = NameStr(opform->oprname);

    /* Print schema name only if it's not pg_catalog */
    if (opform->oprnamespace != PG_CATALOG_NAMESPACE)
    {
        const char *opnspname = get_namespace_name(opform->oprnamespace);
        /* Print fully qualified operator name. */
        appendStringInfo(buf, "OPERATOR(%s.%s)",
                         quote_identifier(opnspname), opname);
    }
    else
    {
        /* Just print operator name. */
        appendStringInfo(buf, "%s", opname);
    }
}

static void
deparseOpExpr(OpExpr *node, HvaultDeparseContext *ctx)
{
    HeapTuple tuple;
    Form_pg_operator form;
    char oprkind;
    ListCell *arg;

    /* Retrieve information about the operator from system catalog. */
    tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
    if (!HeapTupleIsValid(tuple))
    {
        elog(ERROR, "cache lookup failed for operator %u", node->opno);
        return; /* Will never reach here */
    }
    form = (Form_pg_operator) GETSTRUCT(tuple);
    oprkind = form->oprkind;

    /* Sanity check. */
    Assert((oprkind == 'r' && list_length(node->args) == 1) ||
           (oprkind == 'l' && list_length(node->args) == 1) ||
           (oprkind == 'b' && list_length(node->args) == 2));

    /* Always parenthesize the expression. */
    appendStringInfoChar(&ctx->query, '(');

    /* Deparse left operand. */
    if (oprkind == 'r' || oprkind == 'b')
    {
        arg = list_head(node->args);
        deparseExpr(lfirst(arg), ctx);
        appendStringInfoChar(&ctx->query, ' ');
    }

    /* Deparse operator name. */
    deparseOperatorName(&ctx->query, form);

    /* Deparse right operand. */
    if (oprkind == 'l' || oprkind == 'b')
    {
        arg = list_tail(node->args);
        appendStringInfoChar(&ctx->query, ' ');
        deparseExpr(lfirst(arg), ctx);
    }

    appendStringInfoChar(&ctx->query, ')');

    ReleaseSysCache(tuple);
}

static void
deparseDistinctExpr(DistinctExpr *node, HvaultDeparseContext *ctx)
{
    Assert(list_length(node->args) == 2);

    appendStringInfoChar(&ctx->query, '(');
    deparseExpr(linitial(node->args), ctx);
    appendStringInfo(&ctx->query, " IS DISTINCT FROM ");
    deparseExpr(lsecond(node->args), ctx);
    appendStringInfoChar(&ctx->query, ')');
}

static void
deparseArrayRef(ArrayRef *node, HvaultDeparseContext *ctx)
{
    ListCell *lowlist_item;
    ListCell *uplist_item;

    /* Always parenthesize the expression. */
    appendStringInfoChar(&ctx->query, '(');

    /*
     * Deparse and parenthesize referenced array expression first. 
     */
    appendStringInfoChar(&ctx->query, '(');
    deparseExpr(node->refexpr, ctx);
    appendStringInfoChar(&ctx->query, ')');
     

    /* Deparse subscript expressions. */
    lowlist_item = list_head(node->reflowerindexpr);    /* could be NULL */
    foreach(uplist_item, node->refupperindexpr)
    {
        appendStringInfoChar(&ctx->query, '[');
        if (lowlist_item)
        {
            deparseExpr(lfirst(lowlist_item), ctx);
            appendStringInfoChar(&ctx->query, ':');
            lowlist_item = lnext(lowlist_item);
        }
        deparseExpr(lfirst(uplist_item), ctx);
        appendStringInfoChar(&ctx->query, ']');
    }

    appendStringInfoChar(&ctx->query, ')');
}

static void
deparseScalarArrayOpExpr(ScalarArrayOpExpr *node, HvaultDeparseContext *ctx)
{
    HeapTuple tuple;
    Form_pg_operator form;

    /* Retrieve information about the operator from system catalog. */
    tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
    if (!HeapTupleIsValid(tuple))
    {
        elog(ERROR, "cache lookup failed for operator %u", node->opno);
        return; /* Will never reach here */
    }
    form = (Form_pg_operator) GETSTRUCT(tuple);

    /* Sanity check. */
    Assert(list_length(node->args) == 2);

    /* Always parenthesize the expression. */
    appendStringInfoChar(&ctx->query, '(');

    /* Deparse left operand. */
    deparseExpr(linitial(node->args), ctx);
    appendStringInfoChar(&ctx->query, ' ');
    /* Deparse operator name plus decoration. */
    deparseOperatorName(&ctx->query, form);
    appendStringInfo(&ctx->query, " %s (", node->useOr ? "ANY" : "ALL");
    /* Deparse right operand. */
    deparseExpr(lsecond(node->args), ctx);
    appendStringInfoChar(&ctx->query, ')');

    /* Always parenthesize the expression. */
    appendStringInfoChar(&ctx->query, ')');

    ReleaseSysCache(tuple);
}

static void
deparseArrayExpr(ArrayExpr *node, HvaultDeparseContext *ctx)
{
    bool first = true;
    ListCell *l;

    appendStringInfo(&ctx->query, "ARRAY[");
    foreach(l, node->elements)
    {
        if (!first)
            appendStringInfo(&ctx->query, ", ");
        deparseExpr(lfirst(l), ctx);
        first = false;
    }
    appendStringInfoChar(&ctx->query, ']');

    /* If the array is empty, we need an explicit cast to the array type. */
    if (node->elements == NIL)
        appendStringInfo(&ctx->query, "::%s",
                         format_type_with_typemod(node->array_typeid, -1));
}

static void
deparseRelabelType(RelabelType *node, HvaultDeparseContext *ctx)
{
    deparseExpr(node->arg, ctx);
    if (node->relabelformat != COERCE_IMPLICIT_CAST)
        appendStringInfo(&ctx->query, "::%s",
                         format_type_with_typemod(node->resulttype,
                                                  node->resulttypmod));
}

static void
deparseBoolExpr(BoolExpr *node, HvaultDeparseContext *ctx)
{
    const char *op = NULL;
    bool first;
    ListCell *l;

    switch (node->boolop)
    {
        case AND_EXPR:
            op = "AND";
            break;
        case OR_EXPR:
            op = "OR";
            break;
        case NOT_EXPR:
            appendStringInfo(&ctx->query, "(NOT ");
            deparseExpr(linitial(node->args), ctx);
            appendStringInfoChar(&ctx->query, ')');
            return;
        default:
            elog(ERROR, "Unknown boolean expression type %d", node->boolop);
    }

    appendStringInfoChar(&ctx->query, '(');
    first = true;
    foreach(l, node->args)
    {
        if (!first)
            appendStringInfo(&ctx->query, " %s ", op);
        deparseExpr((Expr *) lfirst(l), ctx);
        first = false;
    }
    appendStringInfoChar(&ctx->query, ')');
}

static void
deparseNullTest(NullTest *node, HvaultDeparseContext *ctx)
{
    appendStringInfoChar(&ctx->query, '(');
    deparseExpr(node->arg, ctx);
    if (node->nulltesttype == IS_NULL)
        appendStringInfo(&ctx->query, " IS NULL)");
    else
        appendStringInfo(&ctx->query, " IS NOT NULL)");
}

void
deparseExpr(Expr *node, HvaultDeparseContext *ctx)
{
    if (node == NULL)
        return;

    switch (nodeTag(node))
    {
        case T_Var:
            deparseVar((Var *) node, ctx);
            break;
        case T_Const:
        case T_Param:
            deparseParameter(node, ctx);
            break;
        case T_FuncExpr:
            deparseFuncExpr((FuncExpr *) node, ctx);
            break;
        case T_OpExpr:
            deparseOpExpr((OpExpr *) node, ctx);
            break;
        case T_DistinctExpr:
            deparseDistinctExpr((DistinctExpr *) node, ctx);
            break;
        case T_ArrayRef:
            deparseArrayRef((ArrayRef *) node, ctx);
        case T_ScalarArrayOpExpr:
            deparseScalarArrayOpExpr((ScalarArrayOpExpr *) node, ctx);
            break;
        case T_ArrayExpr:
            deparseArrayExpr((ArrayExpr *) node, ctx);
            break;
        case T_RelabelType:
            deparseRelabelType((RelabelType *) node, ctx);
            break;
        case T_BoolExpr:
            deparseBoolExpr((BoolExpr *) node, ctx);
            break;
        case T_NullTest:
            deparseNullTest((NullTest *) node, ctx);
            break;
        default:
            elog(ERROR, "unsupported expression type for deparse: %d",
                 (int) nodeTag(node));
            break;
    }
}
