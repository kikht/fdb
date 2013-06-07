#include "common.h"
#include "catalog.h"
#include "options.h"
#include "deparse.h"
#include "analyze.h"
#include "utils.h"

#define POINT_SIZE 32
#define FOOTPRINT_SIZE 120
#define STARTUP_COST 10
#define PIXEL_COST 0.001
#define FILE_COST 1

/* 
 * This file includes routines involved in query planning
 */

typedef struct 
{
    PlannerInfo *root;
    RelOptInfo *baserel;
    HvaultQualAnalyzer analyzer;
    List *pathkeys_list;
    List *sort_qual_list;
    List *static_quals;
    List *join_quals;
    List *ec_quals;
    List *considered_relids;
} HvaultPlannerContext;

static inline HvaultTableInfo const * table (HvaultPlannerContext * ctx)
{
    return (HvaultTableInfo const *) ctx->baserel->fdw_private;
}


typedef struct 
{
    HvaultTableInfo const *table;
    List *own_quals;
    List *fdw_expr;
    HvaultCatalogQuery query;
    List *predicates;
} HvaultPathData;

static int
getRowWidth (HvaultColumnType *coltypes, AttrNumber numattrs)
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
makePathKeys (EquivalenceClass *base_ec, 
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
getSortPathKeys (HvaultPlannerContext * ctx)
{
    List *pathkeys;
    ListCell *l, *m;
    Oid intopfamily;
    EquivalenceClass *file_ec = NULL, *line_ec = NULL, 
                     *sample_ec = NULL, *time_ec = NULL;

    foreach(l, ctx->root->eq_classes)
    {
        EquivalenceClass *ec = (EquivalenceClass *) lfirst(l);
        elog(DEBUG2, "processing EquivalenceClass %s", nodeToString(ec));
        if (!bms_is_member(ctx->baserel->relid, ec->ec_relids))
        {
            elog(DEBUG1, "not my relid");
            continue;
        }

        foreach(m, ec->ec_members)
        {
            Var *var;
            EquivalenceMember *em = (EquivalenceMember *) lfirst(m);
            EquivalenceClass **dest_ec = NULL;

            if (!IsA(em->em_expr, Var))
                continue;
        
            var = (Var *) em->em_expr;
            if (ctx->baserel->relid != var->varno)
                continue;
            
            Assert(var->varattno < table(ctx)->natts);
            switch(table(ctx)->coltypes[var->varattno-1])
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
        ctx->pathkeys_list = lappend(ctx->pathkeys_list, pathkeys);
        ctx->sort_qual_list = lappend(ctx->sort_qual_list, "file_id ASC");

        pathkeys = makePathKeys(file_ec, intopfamily, BTGreaterStrategyNumber, 
                                line_ec, sample_ec, intopfamily);
        ctx->pathkeys_list = lappend(ctx->pathkeys_list, pathkeys);
        ctx->sort_qual_list = lappend(ctx->sort_qual_list, "file_id DESC");
    }

    if (time_ec)
    {
        Oid timeopfamily = get_opfamily_oid(
            BTREE_AM_OID, list_make1(makeString("datetime_ops")), false);    

        pathkeys = makePathKeys(time_ec, timeopfamily, BTLessStrategyNumber, 
                                line_ec, sample_ec, intopfamily);
        ctx->pathkeys_list = lappend(ctx->pathkeys_list, pathkeys);
        ctx->sort_qual_list = lappend(ctx->sort_qual_list, "starttime ASC");

        pathkeys = makePathKeys(time_ec, intopfamily, BTGreaterStrategyNumber, 
                                line_ec, sample_ec, intopfamily);
        ctx->pathkeys_list = lappend(ctx->pathkeys_list, pathkeys);
        ctx->sort_qual_list = lappend(ctx->sort_qual_list, "starttime DESC");
    }
}

static void
extractCatalogQuals (HvaultPlannerContext * ctx)
{
    ctx->static_quals = hvaultAnalyzeQuals(ctx->analyzer, 
                                           ctx->baserel->baserestrictinfo);
    ctx->join_quals = hvaultAnalyzeQuals(ctx->analyzer, 
                                         ctx->baserel->joininfo);
    ctx->ec_quals = hvaultAnalyzeECs(ctx->analyzer, ctx->root->eq_classes);
    
}

static void 
addForeignPaths (HvaultPlannerContext * ctx,
                 List * quals,
                 Relids req_outer)
{
    ListCell *l, *m;
    List *predicates, *own_quals, *pred_quals;
    HvaultCatalogQuery query;
    List * fdw_expr;
    
    /* Prepare catalog query */
    own_quals = NIL;
    query = hvaultCatalogInitQuery(table(ctx));
    hvaultCatalogAddProduct(query, "filename");
    foreach(l, quals)
    {
        HvaultQual * qual = lfirst(l);
        hvaultCatalogAddQual(query, qual);
        own_quals = lappend(own_quals, qual->rinfo);
    }
    

    /* Generating predicate info */
    fdw_expr = hvaultCatalogGetParams(query);
    predicates = NIL;
    pred_quals = NIL;
    foreach(l, quals)
    {
        HvaultQual * qual = lfirst(l);
        List * pred = hvaultCreatePredicate(qual, &fdw_expr);
        if (pred != NIL)
        {
            predicates = lappend(predicates, pred);
            pred_quals = lappend(pred_quals, qual->rinfo);
        }
    }

    forboth(l, ctx->pathkeys_list, m, ctx->sort_qual_list)
    {
        List *pathkeys = (List *) lfirst(l);
        char *sort_qual = (char *) lfirst(m);
        Cost catmin, catmax;
        double catrows;
        int catwidth;
        double rows;
        Cost startup_cost, total_cost;
        Selectivity selectivity;

        hvaultCatalogSetSort(query, sort_qual);
        hvaultCatalogGetCosts(query, &catmin, &catmax, &catrows, &catwidth);
        selectivity = clauselist_selectivity(ctx->root, pred_quals, 
                                             ctx->baserel->relid, 
                                             JOIN_INNER, NULL);
        rows = catrows * selectivity * HVAULT_TUPLES_PER_FILE;
        startup_cost = catmin + STARTUP_COST;
        /* TODO: predicate cost */
        total_cost = catmax + STARTUP_COST + catrows * FILE_COST + 
                     rows * PIXEL_COST;

        if (add_path_precheck(ctx->baserel, startup_cost, total_cost, 
                              pathkeys, req_outer))
        {
            ForeignPath *path;
            HvaultPathData *path_data;

            path_data = palloc(sizeof(HvaultPathData));    
            path_data->table = table(ctx);
            path_data->own_quals = own_quals;
            path_data->query = hvaultCatalogCloneQuery(query);
            path_data->fdw_expr = fdw_expr;
            path_data->predicates = predicates;

            path = create_foreignscan_path(ctx->root, ctx->baserel, rows, 
                                           startup_cost, total_cost, pathkeys, 
                                           req_outer, (List *) path_data);
            add_path(ctx->baserel, (Path *) path);
        }
    }
    hvaultCatalogFreeQuery(query);
}

static void   
generateJoinPath (HvaultPlannerContext * ctx, Relids relids)
{
    ListCell *l;
    List *quals = list_copy(ctx->static_quals);
    List *ec_rinfos;
    List *ec_quals;
    Relids req_outer;

    req_outer = bms_copy(relids);
    req_outer = bms_del_member(req_outer, ctx->baserel->relid);
    if (bms_is_empty(req_outer))
    {
        elog(WARNING, "Considering strange relids");
        return;
    }

    foreach(l, ctx->join_quals)
    {
        HvaultQual * qual = lfirst(l);
        RestrictInfo * rinfo = qual->rinfo;
        if (bms_is_subset(rinfo->clause_relids, relids))
            quals = lappend(quals, qual);
    }

    ec_rinfos = generate_join_implied_equalities(ctx->root, relids, 
                                                 req_outer, ctx->baserel);
    ec_quals = hvaultAnalyzeQuals(ctx->analyzer, ec_rinfos);
    quals = list_concat(quals, ec_quals);
    addForeignPaths(ctx, quals, req_outer);
    ctx->considered_relids = lcons(relids, ctx->considered_relids);
}

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
hvaultGetRelSize (PlannerInfo *root, 
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

    baserel->width = getRowWidth(fdw_private->coltypes, fdw_private->natts);
    baserel->rows = num_files * tuples_per_file * scale_factor ; 
    baserel->fdw_private = fdw_private;

    elog(DEBUG2, "GetRelSize: baserestrictinfo: %s\njoininfo: %s",
         nodeToString(baserel->baserestrictinfo),
         nodeToString(baserel->joininfo));
}

void 
hvaultGetPaths (PlannerInfo *root, 
                RelOptInfo *baserel,
                Oid foreigntableid)
{
    ListCell *l, *m, *k;
    HvaultPlannerContext ctx;
    int considered_clauses;

    ctx.root = root;
    ctx.baserel = baserel;
    ctx.pathkeys_list = NIL;  
    ctx.sort_qual_list = NIL;
    ctx.static_quals = NIL;
    ctx.join_quals = NIL;
    ctx.ec_quals = NIL;
    ctx.considered_relids = NIL;
    ctx.analyzer = hvaultAnalyzerInit(table(&ctx));

    /* Process pathkeys */
    if (has_useful_pathkeys(root, baserel))
    {
        getSortPathKeys(&ctx);
    }
    ctx.pathkeys_list = lappend(ctx.pathkeys_list, NIL);
    ctx.sort_qual_list = lappend(ctx.sort_qual_list, NULL);

    extractCatalogQuals(&ctx);
    /* Create simple unparametrized path */
    addForeignPaths(&ctx, ctx.static_quals, NULL);
    
    /* Create parametrized join paths */
    considered_clauses = list_length(ctx.join_quals) + 
                         list_length(ctx.ec_quals);
    foreach(l, ctx.join_quals)
    {
        HvaultQual *qual = lfirst(l);
        Relids clause_relids = qual->rinfo->clause_relids;
        if (bms_equal_any(clause_relids, ctx.considered_relids))
            continue;

        foreach(m, ctx.considered_relids)
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
            if (list_length(ctx.considered_relids) >= 10 * considered_clauses)
                break;       

            generateJoinPath(&ctx, bms_union(oldrelids, clause_relids));
        }
        generateJoinPath(&ctx, clause_relids);
    }

    /* Derive join paths from EC */
    foreach(l, ctx.ec_quals)
    {
        HvaultEC *hec = lfirst(l);
        
        Assert(hec->var);

        foreach(m, hec->ec->ec_members)
        {
            EquivalenceMember *em = (EquivalenceMember *) lfirst(m);
            Var *var;
            Relids relids;

            if (!IsA(em->em_expr, Var))
                continue;

            var = (Var *) em->em_expr;
            if (var->varno == baserel->relid)
                continue;

            relids = bms_make_singleton(hec->var->varno);
            relids = bms_add_member(relids, var->varno);
            if (bms_equal_any(relids, ctx.considered_relids))
                continue;

            foreach(k, ctx.considered_relids)
            {
                Relids oldrelids = (Relids) lfirst(k);
                Relids union_relids;
                
                if (bms_is_member(var->varno, oldrelids))                
                    continue;

                if (list_length(ctx.considered_relids) 
                         >= 10 * considered_clauses)
                    break;

                union_relids = bms_copy(oldrelids);
                union_relids = bms_add_member(union_relids, var->varno);
                generateJoinPath(&ctx, union_relids);
            }
            generateJoinPath(&ctx, relids);
        }
    }
    hvaultAnalyzerFree(ctx.analyzer);
}

ForeignScan *
hvaultGetPlan (PlannerInfo *root, 
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
