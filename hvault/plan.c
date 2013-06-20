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
    List * pathkeys;
    char const * column;
    bool desc;
} SortInfo;

typedef struct 
{
    PlannerInfo *root;
    RelOptInfo *baserel;
    Oid foreigntableid;
    HvaultCatalogQuery query;

    TupleDesc tupdesc;
    int tuple_width;
    HvaultTableInfo table;
    
    HvaultQualAnalyzer analyzer;
    List *sort_list;
    List *static_quals;
    List *join_quals;
    List *ec_quals;
    List *considered_relids;
} HvaultPlannerContext;

static inline HvaultTableInfo * table (HvaultPlannerContext * ctx)
{
    return &ctx->table;
}


typedef struct 
{
    HvaultTableInfo const *table;
    List *own_quals;
    List *fdw_expr;
    List *packed_query;
    List *predicates;
} HvaultPathData;

static PathKey * makeIndexPathkeys (EquivalenceClass *ec, Oid intopfamily)
{
    PathKey *pk = makeNode(PathKey);
    pk->pk_eclass = ec;
    pk->pk_nulls_first = false;
    pk->pk_strategy = BTLessStrategyNumber;
    pk->pk_opfamily = intopfamily;
    return pk;
}

typedef struct CatalogEC CatalogEC;
struct CatalogEC
{
    EquivalenceClass *ec;
    AttrNumber attnum;
    CatalogEC *next;
};

static void   
getSortPathKeys (HvaultPlannerContext * ctx)
{
    ListCell *l, *m;
    PathKey *line_pk = NULL, *sample_pk = NULL, *index_pk = NULL, *base_pk;
    CatalogEC * catalog_ec;
    CatalogEC * cur_ec = NULL;
    Oid intopfamily;
    List *tails = NIL;

    intopfamily  = get_opfamily_oid(BTREE_AM_OID, 
                                    list_make1(makeString("integer_ops")), 
                                    false);
    catalog_ec = palloc0(sizeof(CatalogEC) * table(ctx)->natts);
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

            if (!IsA(em->em_expr, Var))
                continue;
        
            var = (Var *) em->em_expr;
            if (ctx->baserel->relid != var->varno)
                continue;
            
            Assert(var->varattno <= table(ctx)->natts);
            switch (table(ctx)->columns[var->varattno-1].type)
            {
                case HvaultColumnIndex:
                    if (index_pk)
                        elog(ERROR, "Duplicate index column");
                    index_pk = makeIndexPathkeys(ec, intopfamily);
                    break;
                case HvaultColumnLineIdx:
                    if (line_pk)
                        elog(ERROR, "Duplicate line_idx column");
                    line_pk = makeIndexPathkeys(ec, intopfamily);
                    break;
                case HvaultColumnSampleIdx:
                    if (sample_pk)
                        elog(ERROR, "Duplicate sample_idx column");
                    sample_pk = makeIndexPathkeys(ec, intopfamily);
                    break;
                case HvaultColumnCatalog:
                    if (catalog_ec[var->varattno-1].ec == NULL)
                    {
                        catalog_ec[var->varattno-1].ec = ec;
                        catalog_ec[var->varattno-1].attnum = var->varattno-1;
                        catalog_ec[var->varattno-1].next = cur_ec;
                        cur_ec = catalog_ec + var->varattno - 1;
                    }
                    else 
                    {
                        elog(ERROR, "Duplicate catalog column %s", 
                             table(ctx)->columns[var->varattno-1].cat_name);
                    }
                    break;
                default:
                    /*nop*/
                    break;
            }
        }
    }

    if (index_pk)
        tails = lappend(tails, list_make1(index_pk));

    if (line_pk)
    {
        if (sample_pk)
            tails = lappend(tails, list_make2(line_pk, sample_pk));
        else
            tails = lappend(tails, list_make1(line_pk));
    }

    if (list_length(tails) == 0)
        tails = list_make1(NIL);


    base_pk = makeNode(PathKey);
    base_pk->pk_nulls_first = false;
    for ( ; cur_ec != NULL ; cur_ec = cur_ec->next)
    {
        base_pk->pk_eclass = cur_ec->ec;
        if (!base_pk->pk_eclass)
            continue;

        foreach(l, base_pk->pk_eclass->ec_opfamilies)
        {
            base_pk->pk_opfamily = lfirst_oid(l);
            foreach(m, tails)
            {
                List *tail = lfirst(m);
                int desc;
                for (desc = 0; desc < 2; desc++)
                {
                    SortInfo * sort;
                    base_pk->pk_strategy = desc ? BTLessStrategyNumber : 
                                                  BTGreaterStrategyNumber;
                    sort = palloc(sizeof(SortInfo));
                    sort->column = table(ctx)->columns[cur_ec->attnum].cat_name;
                    sort->desc = desc;
                    sort->pathkeys = list_make1(copyObject(base_pk));
                    sort->pathkeys = list_concat(sort->pathkeys, tail);
                }
            }
        }
    }

    pfree(base_pk);
    pfree(catalog_ec);
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
    ListCell *l;
    List *predicates, *own_quals, *pred_quals;
    HvaultCatalogQuery query;
    List * fdw_expr;
    
    /* Prepare catalog query */
    own_quals = NIL;
    query = hvaultCatalogCloneQuery(ctx->query);
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

    foreach(l, ctx->sort_list)
    {
        SortInfo * sort = lfirst(l);
        Cost catmin, catmax;
        double catrows;
        int catwidth;
        double rows;
        Cost startup_cost, total_cost;
        Selectivity selectivity;

        hvaultCatalogSetSort(query, sort->column, sort->desc);
        hvaultCatalogGetCosts(query, &catmin, &catmax, &catrows, &catwidth);
        selectivity = clauselist_selectivity(ctx->root, pred_quals, 
                                             ctx->baserel->relid, 
                                             JOIN_INNER, NULL);
        /* TODO: get rid of HVAULT_TUPLES_PER_FILE as it depends on driver */
        rows = catrows * selectivity * HVAULT_TUPLES_PER_FILE;
        startup_cost = catmin + STARTUP_COST;
        /* TODO: predicate cost */
        total_cost = catmax + STARTUP_COST + catrows * FILE_COST + 
                     rows * PIXEL_COST;

        if (add_path_precheck(ctx->baserel, startup_cost, total_cost, 
                              sort->pathkeys, req_outer))
        {
            ForeignPath *path;
            HvaultPathData *path_data;

            path_data = palloc(sizeof(HvaultPathData));    
            path_data->table = table(ctx);
            path_data->own_quals = own_quals;
            path_data->packed_query = hvaultCatalogPackQuery(query);
            path_data->fdw_expr = fdw_expr;
            path_data->predicates = predicates;

            path = create_foreignscan_path(ctx->root, ctx->baserel, rows, 
                                           startup_cost, total_cost, 
                                           sort->pathkeys, req_outer, 
                                           (List *) path_data);
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

static void 
processUsedColumn (Var * var, void * arg)
{
    HvaultPlannerContext * ctx = arg;
    List * options;
    int attlen;
    HvaultColumnInfo * colinfo = table(ctx)->columns + var->varattno-1;

    if (colinfo->type != HvaultColumnNull)
    {
        /* Already processed */
        return;
    }

    options = GetForeignColumnOptions(ctx->foreigntableid, var->varattno);
    colinfo->type = hvaultGetColumnType(
        defFindByName(options, HVAULT_COLUMN_OPTION_TYPE));
    colinfo->cat_name = defFindStringByName(options, 
                                            HVAULT_COLUMN_OPTION_CATNAME);
    if (colinfo->cat_name != NULL && colinfo->type >= HvaultColumnFootprint 
                                  && colinfo->type <= HvaultColumnCatalog)
    {
        hvaultCatalogAddColumn(ctx->query, colinfo->cat_name);
    }

    switch (colinfo->type) 
    {
        case HvaultColumnNull:
            /* nop */
            break;
        case HvaultColumnIndex:
        case HvaultColumnLineIdx:
        case HvaultColumnSampleIdx:
            ctx->tuple_width += 4;
            break;
        case HvaultColumnPoint:
            ctx->tuple_width += POINT_SIZE;
            break;
        case HvaultColumnFootprint:
            ctx->tuple_width += FOOTPRINT_SIZE;
            break;
        case HvaultColumnCatalog:
        case HvaultColumnDataset:
            /* get width from datatype */
            attlen = ctx->tupdesc->attrs[var->varattno-1]->attlen;
            ctx->tuple_width += attlen > 0 ? attlen : sizeof(Datum);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                    errmsg("Undefined column type")));
    }
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
    HvaultPlannerContext * ctx = palloc0(sizeof(HvaultPlannerContext));
    Relation rel;

    ctx->root = root;
    ctx->baserel = baserel;
    ctx->foreigntableid = foreigntableid;


    rel = heap_open(foreigntableid, AccessShareLock);
    ctx->tupdesc = RelationGetDescr(rel);
    ctx->tupdesc = CreateTupleDescCopy(ctx->tupdesc);
    heap_close(rel, AccessShareLock);    

    table(ctx)->relid = baserel->relid;
    table(ctx)->catalog = hvaultGetTableOptionString(foreigntableid, "catalog");
    table(ctx)->natts = ctx->tupdesc->natts;
    table(ctx)->columns = palloc0(sizeof(HvaultColumnInfo) * table(ctx)->natts);

    ctx->query = hvaultCatalogInitQuery(table(ctx));
    ctx->tuple_width = 0;
    hvaultAnalyzeUsedColumns((Node *) baserel->baserestrictinfo, baserel->relid, 
                             processUsedColumn, ctx);
    hvaultAnalyzeUsedColumns((Node *) baserel->joininfo, baserel->relid, 
                             processUsedColumn, ctx);
    hvaultAnalyzeUsedColumns((Node *) root->eq_classes, baserel->relid, 
                             processUsedColumn, ctx);

    /* TODO: Use constant catalog quals for better estimate */
    /* Use driver-dependent tuples per file estimate */
    baserel->rows = hvaultGetNumFiles(table(ctx)->catalog) 
                  * HVAULT_TUPLES_PER_FILE;
    baserel->width = ctx->tuple_width;
    baserel->fdw_private = ctx;
}

void 
hvaultGetPaths (PlannerInfo *root, 
                RelOptInfo *baserel,
                Oid foreigntableid)
{
    ListCell *l, *m, *k;
    int considered_clauses;
    SortInfo * no_sort_info = palloc(sizeof(SortInfo));
    HvaultPlannerContext * ctx = baserel->fdw_private;

    ctx->sort_list = NIL;
    ctx->static_quals = NIL;
    ctx->join_quals = NIL;
    ctx->ec_quals = NIL;
    ctx->considered_relids = NIL;
    ctx->analyzer = hvaultAnalyzerInit(table(ctx));

    /* Process pathkeys */
    if (has_useful_pathkeys(root, baserel))
    {
        getSortPathKeys(ctx);
    }
    no_sort_info->pathkeys = NIL;
    no_sort_info->column = NULL;
    ctx->sort_list = lappend(ctx->sort_list, no_sort_info);

    extractCatalogQuals(ctx);
    /* Create simple unparametrized path */
    addForeignPaths(ctx, ctx->static_quals, NULL);
    
    /* Create parametrized join paths */
    considered_clauses = list_length(ctx->join_quals) + 
                         list_length(ctx->ec_quals);
    foreach(l, ctx->join_quals)
    {
        HvaultQual *qual = lfirst(l);
        Relids clause_relids = qual->rinfo->clause_relids;
        if (bms_equal_any(clause_relids, ctx->considered_relids))
            continue;

        foreach(m, ctx->considered_relids)
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
            if (list_length(ctx->considered_relids) >= 10 * considered_clauses)
                break;       

            generateJoinPath(ctx, bms_union(oldrelids, clause_relids));
        }
        generateJoinPath(ctx, clause_relids);
    }

    /* Derive join paths from EC */
    foreach(l, ctx->ec_quals)
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
            if (bms_equal_any(relids, ctx->considered_relids))
                continue;

            foreach(k, ctx->considered_relids)
            {
                Relids oldrelids = (Relids) lfirst(k);
                Relids union_relids;
                
                if (bms_is_member(var->varno, oldrelids))                
                    continue;

                if (list_length(ctx->considered_relids) 
                         >= 10 * considered_clauses)
                    break;

                union_relids = bms_copy(oldrelids);
                union_relids = bms_add_member(union_relids, var->varno);
                generateJoinPath(ctx, union_relids);
            }
            generateJoinPath(ctx, relids);
        }
    }
    hvaultAnalyzerFree(ctx->analyzer);
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
    List * coltypes;
    int i;

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

    elog(DEBUG3, "GetPlan: scan_cl: %s\nrest_cl: %s",
         nodeToString(scan_clauses),
         nodeToString(rest_clauses));
    elog(DEBUG3, "GetPlan: tlist: %s", nodeToString(tlist));

    coltypes = NIL;
    for (i = 0; i < fdw_private->table->natts; i++)
    {
        coltypes = lappend_int(coltypes, fdw_private->table->columns[i].type);
    }

    /* store fdw_private in List */
    fdw_plan_private = list_make3(fdw_private->packed_query, 
                                  fdw_private->predicates,
                                  coltypes);

    return make_foreignscan(tlist, rest_clauses, baserel->relid, 
                            fdw_private->fdw_expr, fdw_plan_private);
}
