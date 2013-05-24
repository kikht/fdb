#include <postgres.h>
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <foreign/foreign.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pg_list.h>
#include <nodes/primnodes.h>
#include <nodes/relation.h>
#include <optimizer/cost.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/planmain.h>
#include <tcop/tcopprot.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/rel.h>

#include "catalog.h"
#include "deparse.h"
#include "hvault.h"
#include "utils.h"

#define POINT_SIZE 32
#define FOOTPRINT_SIZE 120
#define STARTUP_COST 10
#define PIXEL_COST 0.001
#define FILE_COST 1

#define GEOMETRY_OP_QUERY \
    "SELECT o.oid from pg_opclass c" \
        " JOIN pg_amop ao ON c.opcfamily = ao.amopfamily" \
        " JOIN pg_operator o ON ao.amopopr = o.oid " \
        " WHERE c.opcname = 'gist_geometry_ops_2d' AND o.oprname = $1"

/* 
 * This file includes routines involved in query planning
 */



typedef struct 
{
    HvaultTableInfo const *table;
    List *own_quals;
    List *fdw_expr;
    // char const * filter;
    // char const * sort;
    HvaultCatalogQuery query;
    List *predicates;
} HvaultPathData;





static int  get_row_width(HvaultColumnType *coltypes, AttrNumber numattrs);
static void getSortPathKeys(PlannerInfo const *root,
                            RelOptInfo const *baserel,
                            List **pathkeys_list,
                            List **sort_part);
static bool bms_equal_any(Relids relids, List *relids_list);
static void getGeometryOpers(HvaultTableInfo *table);
static void extractCatalogQuals(PlannerInfo *root,
                                RelOptInfo *baserel,
                                HvaultTableInfo const *table,
                                List **catalog_quals,
                                List **catalog_joins,
                                List **catalog_ec,
                                List **catalog_ec_vars,
                                List **footprint_quals,
                                List **footprint_joins);

static void addForeignPaths(PlannerInfo *root, 
                            RelOptInfo *baserel, 
                            HvaultTableInfo const *table,
                            List *pathkeys_list,
                            List *sort_qual_list,
                            List *catalog_quals,
                            List *footprint_quals,
                            Relids req_outer);
static void generateJoinPath(PlannerInfo *root, 
                             RelOptInfo *baserel, 
                             HvaultTableInfo const *table,
                             List *pathkeys_list,
                             List *sort_qual_list,
                             List *catalog_quals,
                             List *catalog_joins,
                             List *footprint_quals,
                             List *footprint_joins,
                             Relids relids,
                             List **considered_relids);
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
    fdw_private->coltypes = hvaultGetUsedColumns(root, baserel, foreigntableid, 
                                                 fdw_private->natts);
    
    fdw_private->catalog = hvaultGetTableOptionString(foreigntableid, 
                                                      "catalog");

    /* TODO: Use constant catalog quals for better estimate */
    num_files = hvaultGetNumFiles(fdw_private->catalog);
    tuples_per_file = HVAULT_TUPLES_PER_FILE;
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
    ListCell *l, *m, *k, *s;
    List *pathkeys_list = NIL;  
    List *sort_qual_list = NIL;
    List *catalog_quals = NIL;
    List *catalog_joins = NIL;
    List *catalog_ec = NIL;
    List *catalog_ec_vars = NIL;
    List *footprint_quals = NIL;
    List *footprint_joins = NIL;
    List *considered_relids = NIL;
    List *all_joins = NIL;
    int considered_clauses;

    /* Process pathkeys */
    if (has_useful_pathkeys(root, baserel))
    {
        getSortPathKeys(root, baserel, &pathkeys_list, &sort_qual_list);
    }
    if (list_length(pathkeys_list) == 0)
    {
        pathkeys_list = list_make1(NIL);
        sort_qual_list = list_make1(NULL);
    }

    getGeometryOpers(fdw_private);
    extractCatalogQuals(root, baserel, fdw_private, &catalog_quals, 
                        &catalog_joins, &catalog_ec, &catalog_ec_vars,
                        &footprint_quals, &footprint_joins);
    
    /* Create simple unparametrized path */
    addForeignPaths(root, baserel, fdw_private, pathkeys_list, sort_qual_list, 
                    catalog_quals, footprint_quals, NULL);
    
    /* Create parametrized join paths */
    all_joins = list_copy(catalog_joins);
    foreach(l, footprint_joins)
    {
        HvaultGeomOpQual *qual = lfirst(l);
        all_joins = lappend(all_joins, qual->rinfo);
    }
    considered_clauses = list_length(all_joins) + list_length(catalog_ec);
    foreach(l, all_joins)
    {
        RestrictInfo *rinfo = lfirst(l);
        Relids clause_relids = rinfo->clause_relids;
        if (bms_equal_any(clause_relids, considered_relids))
            continue;

        foreach(m, considered_relids)
        {
            Relids oldrelids = (Relids) lfirst(m);

            /*
             * If either is a subset of the other, no new set is possible.
             * This isn't a complete test for redundancy, but it's easy and
             * cheap.  get_join_index_paths will check more carefully if we
             * already generated the same relids set.
             */
            if (bms_subset_compare(clause_relids, oldrelids) != BMS_DIFFERENT)
                continue;

            /*
             * If the number of relid sets considered exceeds our heuristic
             * limit, stop considering combinations of clauses.  We'll still
             * consider the current clause alone, though (below this loop).
             */
            if (list_length(considered_relids) >= 10 * considered_clauses)
                break;       

            generateJoinPath(root, baserel, fdw_private, 
                             pathkeys_list, sort_qual_list,
                             catalog_quals, catalog_joins, 
                             footprint_quals, footprint_joins,
                             bms_union(oldrelids, clause_relids),
                             &considered_relids);
        }

        generateJoinPath(root, baserel, fdw_private,
                         pathkeys_list, sort_qual_list,
                         catalog_quals, catalog_joins, 
                         footprint_quals, footprint_joins,
                         clause_relids,
                         &considered_relids);
    }

    /* Derive join paths from EC */
    forboth(l, catalog_ec, s, catalog_ec_vars)
    {
        EquivalenceClass *ec = (EquivalenceClass *) lfirst(l);
        Var *catalog_var = (Var *) lfirst(s);
        if (catalog_var == NULL)
            continue;

        foreach(m, ec->ec_members)
        {
            EquivalenceMember *em = (EquivalenceMember *) lfirst(m);
            Var *var;
            Relids relids;

            if (!IsA(em->em_expr, Var))
                continue;

            var = (Var *) em->em_expr;
            if (var->varno == baserel->relid)
                continue;

            relids = bms_make_singleton(catalog_var->varno);
            relids = bms_add_member(relids, var->varno);
            if (bms_equal_any(relids, considered_relids))
                continue;

            foreach(k, considered_relids)
            {
                Relids oldrelids = (Relids) lfirst(k);
                Relids union_relids;
                
                if (bms_is_member(var->varno, oldrelids))                
                    continue;

                if (list_length(considered_relids) >= 10 * considered_clauses)
                    break;

                union_relids = bms_copy(oldrelids);
                union_relids = bms_add_member(union_relids, var->varno);
                generateJoinPath(root, baserel, fdw_private,
                                 pathkeys_list, sort_qual_list,
                                 catalog_quals, catalog_joins, 
                                 footprint_quals, footprint_joins,
                                 union_relids, &considered_relids);
            }

            generateJoinPath(root, baserel, fdw_private, 
                             pathkeys_list, sort_qual_list,
                             catalog_quals, catalog_joins, 
                             footprint_quals, footprint_joins,
                             relids, &considered_relids);        
        }
    }
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
    List *coltypes;
    AttrNumber attnum;
    StringInfoData query;

    elog(DEBUG1, "Selected path quals: %s", 
         nodeToString(fdw_private->own_quals));


    /* Extract clauses that need to be checked externally */
    foreach(l, scan_clauses)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
        if (!list_member_ptr(fdw_private->own_quals, rinfo))
        {
            rest_clauses = lappend(rest_clauses, rinfo);
        }
        else
        {
            elog(DEBUG1, "Skipping external clause %s", 
                 nodeToString(rinfo->clause));
        }
    }
    rest_clauses = extract_actual_clauses(rest_clauses, false);

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
                               hvaultCatalogPackQuery(fdw_private->query));
    fdw_plan_private = lappend(fdw_plan_private, coltypes);
    fdw_plan_private = lappend(fdw_plan_private, fdw_private->predicates);

    return make_foreignscan(tlist, rest_clauses, baserel->relid, 
                            fdw_private->fdw_expr, fdw_plan_private);
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
            case HvaultColumnInt8Val:
                width += sizeof(int16_t);
                break;
            case HvaultColumnInt16Val:
                width += sizeof(int16_t);
                break;
            case HvaultColumnInt32Val:
                width += sizeof(int32_t);
                break;
            case HvaultColumnInt64Val:
                width += sizeof(int64_t);
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

static List *
makePathKeys(EquivalenceClass *base_ec, 
             Oid base_opfamily, 
             int base_strategy,
             EquivalenceClass *line_ec,
             EquivalenceClass *sample_ec,
             Oid intopfamily)
{
    List *result = NIL;
    PathKey *pk = makeNode(PathKey);
    pk->pk_eclass = base_ec;
    pk->pk_nulls_first = false;
    pk->pk_strategy = base_strategy;
    pk->pk_opfamily = base_opfamily;
    result = lappend(result, pk);

    if (line_ec)
    {
        PathKey *pk = makeNode(PathKey);
        pk->pk_eclass = line_ec;
        pk->pk_nulls_first = false;
        pk->pk_strategy = BTLessStrategyNumber;
        pk->pk_opfamily = intopfamily;
        result = lappend(result, pk);

        if (sample_ec)
        {
            PathKey *pk = makeNode(PathKey);
            pk->pk_eclass = sample_ec;
            pk->pk_nulls_first = false;
            pk->pk_strategy = BTLessStrategyNumber;
            pk->pk_opfamily = intopfamily;
            result = lappend(result, pk);            
        }
    }

    return result;
}

static void   
getSortPathKeys(PlannerInfo const *root,
                RelOptInfo const *baserel,
                List **pathkeys_list,
                List **sort_part)
{
    List *pathkeys;
    ListCell *l, *m;
    Oid intopfamily;
    HvaultTableInfo *fdw_private = (HvaultTableInfo *) baserel->fdw_private;
    EquivalenceClass *file_ec = NULL, *line_ec = NULL, 
                     *sample_ec = NULL, *time_ec = NULL;

    foreach(l, root->eq_classes)
    {
        EquivalenceClass *ec = (EquivalenceClass *) lfirst(l);
        elog(DEBUG2, "processing EquivalenceClass %s", nodeToString(ec));
        if (!bms_is_member(baserel->relid, ec->ec_relids))
        {
            elog(DEBUG1, "not my relid");
            continue;
        }

        foreach(m, ec->ec_members)
        {
            Var *var;
            EquivalenceMember *em = (EquivalenceMember *) lfirst(m);

            if (!IsA(em->em_expr, Var))
                continue;
        
            var = (Var *) em->em_expr;
            if (baserel->relid != var->varno)
                continue;
            
            EquivalenceClass **dest_ec = NULL;
            Assert(var->varattno < fdw_private->natts);
            switch(fdw_private->coltypes[var->varattno-1])
            {
                case HvaultColumnFileIdx:
                    dest_ec = &file_ec;
                    break;
                case HvaultColumnLineIdx:
                    dest_ec = &line_ec;
                    break;
                case HvaultColumnSampleIdx:
                    dest_ec = &sample_ec;
                    break;
                case HvaultColumnTime:
                    dest_ec = &time_ec;
                    break;
                default:
                    /* nop */
                    break;
            }

            if (dest_ec)
            {
                if (*dest_ec)
                {
                    elog(ERROR, "Duplicate special column");
                    return; /* Will never reach here */
                }

                *dest_ec = ec;
            }
        }
    }

    intopfamily  = get_opfamily_oid(BTREE_AM_OID, 
                                    list_make1(makeString("integer_ops")), 
                                    false);
    if (file_ec)
    {
        pathkeys = makePathKeys(file_ec, intopfamily, BTLessStrategyNumber, 
                                line_ec, sample_ec, intopfamily);
        *pathkeys_list = lappend(*pathkeys_list, pathkeys);
        *sort_part = lappend(*sort_part, "file_id ASC");

        pathkeys = makePathKeys(file_ec, intopfamily, BTGreaterStrategyNumber, 
                                line_ec, sample_ec, intopfamily);
        *pathkeys_list = lappend(*pathkeys_list, pathkeys);
        *sort_part = lappend(*sort_part, "file_id DESC");
    }

    if (time_ec)
    {
        Oid timeopfamily = get_opfamily_oid(
            BTREE_AM_OID, list_make1(makeString("datetime_ops")), false);    

        pathkeys = makePathKeys(time_ec, timeopfamily, BTLessStrategyNumber, 
                                line_ec, sample_ec, intopfamily);
        *pathkeys_list = lappend(*pathkeys_list, pathkeys);
        *sort_part = lappend(*sort_part, "starttime ASC");

        pathkeys = makePathKeys(time_ec, intopfamily, BTGreaterStrategyNumber, 
                                line_ec, sample_ec, intopfamily);
        *pathkeys_list = lappend(*pathkeys_list, pathkeys);
        *sort_part = lappend(*sort_part, "starttime DESC");
    }
}



static void
extractCatalogQuals(PlannerInfo *root,
                    RelOptInfo *baserel,
                    HvaultTableInfo const *table,
                    List **catalog_quals,
                    List **catalog_joins,
                    List **catalog_ec,
                    List **catalog_ec_vars,
                    List **footprint_quals,
                    List **footprint_joins)
{
    

    
    ListCell *l;
    HvaultGeomOpQual *fqual = palloc(sizeof(HvaultGeomOpQual));

    foreach(l, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = lfirst(l);
        if (isCatalogQual(rinfo->clause, table))
        {
            elog(DEBUG2, "Detected catalog qual %s", 
                 nodeToString(rinfo->clause));
            *catalog_quals = lappend(*catalog_quals, rinfo);
        } 
        else if (isFootprintQual(rinfo->clause, table, fqual))
        {
            elog(DEBUG2, "Detected footprint qual %s", 
                 nodeToString(rinfo->clause));
            fqual->rinfo = rinfo;
            *footprint_quals = lappend(*footprint_quals, fqual);
            fqual = palloc(sizeof(HvaultGeomOpQual));
        }
    }

    foreach(l, baserel->joininfo)
    {
        RestrictInfo *rinfo = lfirst(l);
        if (isCatalogQual(rinfo->clause, table))
        {
            *catalog_joins = lappend(*catalog_joins, rinfo);
        }
        else if (isFootprintQual(rinfo->clause, table, fqual))
        {
            elog(DEBUG2, "Detected footprint join %s", 
                 nodeToString(rinfo->clause));
            fqual->rinfo = rinfo;
            *footprint_joins = lappend(*footprint_joins, fqual);
            fqual = palloc(sizeof(HvaultGeomOpQual));
        }
    }
    pfree(fqual);

    foreach(l, root->eq_classes)
    {
        EquivalenceClass *ec = (EquivalenceClass *) lfirst(l);
        Var *catalog_var = isCatalogJoinEC(ec, table);
        if (catalog_var != NULL)
        {
            *catalog_ec = lappend(*catalog_ec, ec);
            *catalog_ec_vars = lappend(*catalog_ec_vars, catalog_var);
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

static void
getQueryCosts(char const *query,
              int nargs, 
              Oid *argtypes, 
              Cost *startup_cost,
              Cost *total_cost,
              double *plan_rows,
              int *plan_width)
{
    MemoryContext oldmemctx, memctx;
    List *parsetree, *stmt_list;
    PlannedStmt *plan;

    memctx = AllocSetContextCreate(CurrentMemoryContext, 
                                   "hvaultQueryCosts context", 
                                   ALLOCSET_DEFAULT_MINSIZE,
                                   ALLOCSET_DEFAULT_INITSIZE,
                                   ALLOCSET_DEFAULT_MAXSIZE);
    oldmemctx = MemoryContextSwitchTo(memctx);

    parsetree = pg_parse_query(query);
    Assert(list_length(parsetree) == 1);
    stmt_list = pg_analyze_and_rewrite(linitial(parsetree), 
                                       query, 
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

static List *
predicateToList(HvaultGeomPredicate *p)
{
    return list_make4_int(p->coltype, p->op, p->argno, p->isneg);
}

static void addForeignPaths(PlannerInfo *root, 
                           RelOptInfo *baserel, 
                           HvaultTableInfo const *table,
                           List *pathkeys_list,
                           List *sort_qual_list,
                           List *catalog_quals,
                           List *footprint_quals,
                           Relids req_outer)
{
    double rows;
    Cost startup_cost, total_cost;
    ListCell *l, *m;
    Cost catmin, catmax;
    double catrows;
    int catwidth;
    List *predicates, *own_quals, *pred_quals;
    Selectivity selectivity;
    HvaultCatalogQuery query;
    List * fdw_expr;

    own_quals = list_copy(catalog_quals);
    /* Prepare catalog query */
    query = hvaultCatalogInitQuery(table->catalog, 
                                   table->coltypes, 
                                   table->natts);
    hvaultCatalogAddProduct(query, "file");
    foreach(l, catalog_quals)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
        hvaultCatalogAddSimpleQual(query, rinfo->clause);
    }

    foreach(l, footprint_quals)
    {
        HvaultGeomOpQual *qual = (HvaultGeomOpQual *) lfirst(l);
        if (qual->catalog_pred.op != HvaultGeomInvalidOp)
        {
            hvaultCatalogAddGeometryQual(query, qual);
        }
    }
    fdw_expr = hvaultCatalogGetParams(query);


    predicates = NIL;
    pred_quals = NIL;
    foreach(l, footprint_quals)
    {
        HvaultGeomOpQual *qual = (HvaultGeomOpQual *) lfirst(l);
        HvaultGeomPredicate pred;

        pred.coltype = qual->coltype;
        pred.op = qual->pred.op;
        pred.isneg = qual->pred.isneg;
        pred.argno = list_append_unique_pos(&fdw_expr, qual->arg);
        predicates = lappend(predicates, predicateToList(&pred));
        own_quals = lappend(own_quals, qual->rinfo);
        pred_quals = lappend(pred_quals, qual->rinfo);
    }

    hvaultCatalogGetCosts(query, &catmin, &catmax, &catrows, &catwidth);
    selectivity = clauselist_selectivity(root, pred_quals, baserel->relid, 
                                         JOIN_INNER, NULL);
    rows = catrows * selectivity * HVAULT_TUPLES_PER_FILE;
    startup_cost = catmin + STARTUP_COST;
    /* TODO: predicate cost */
    total_cost = catmax + STARTUP_COST + catrows * FILE_COST + 
                 rows * PIXEL_COST;
    forboth(l, pathkeys_list, m, sort_qual_list)
    {
        List *pathkeys = (List *) lfirst(l);
        char *sort_qual = (char *) lfirst(m);

        if (add_path_precheck(baserel, startup_cost, total_cost, 
                              pathkeys, req_outer))
        {
            ForeignPath *path;
            HvaultPathData *path_data;

            path_data = palloc(sizeof(HvaultPathData));    
            path_data->table = table;
            path_data->own_quals = own_quals;
            path_data->query = hvaultCatalogCloneQuery(query);
            hvaultCatalogAddSortQual(query, sort_qual);
            path_data->fdw_expr = fdw_expr;
            path_data->predicates = predicates;

            path = create_foreignscan_path(root, baserel, rows, 
                                           startup_cost, total_cost, pathkeys, 
                                           req_outer, (List *) path_data);
            add_path(baserel, (Path *) path);
        }
    }

    hvaultCatalogFreeQuery(query);
}

static void   
generateJoinPath(PlannerInfo *root, 
                 RelOptInfo *baserel, 
                 HvaultTableInfo const *table,
                 List *pathkeys_list,
                 List *sort_qual_list,
                 List *catalog_quals,
                 List *catalog_joins,
                 List *footprint_quals,
                 List *footprint_joins,
                 Relids relids,
                 List **considered_relids)
{
    ListCell *l;
    List *quals = list_copy(catalog_quals);
    List *fquals = list_copy(footprint_quals);
    List *ec_quals;
    Relids req_outer;

    req_outer = bms_copy(relids);
    req_outer = bms_del_member(req_outer, baserel->relid);
    if (bms_is_empty(req_outer))
    {
        elog(WARNING, "Considering strange relids");
        return;
    }

    foreach(l, catalog_joins)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
        if (bms_is_subset(rinfo->clause_relids, relids))
            quals = lappend(quals, rinfo);
    }

    foreach(l, footprint_joins)
    {
        HvaultGeomOpQual *qual = (HvaultGeomOpQual *) lfirst(l);
        if (bms_is_subset(qual->rinfo->clause_relids, relids))
            fquals = lappend(fquals, qual);   
    }

    ec_quals = generate_join_implied_equalities(root, relids, req_outer, 
                                                baserel);
    quals = list_concat(quals, ec_quals);
    addForeignPaths(root, baserel, table, pathkeys_list, sort_qual_list, 
                    quals, fquals, req_outer);

    *considered_relids = lcons(relids, *considered_relids);
}



