#include "analyze.h"
#include "utils.h"
#include "deparse.h"

char const * hvaultGeomopstr[HvaultGeomNumAllOpers] = {
    /* HvaultGeomOverlaps  -> */ "&&",
    /* HvaultGeomContains  -> */ "~",
    /* HvaultGeomWithin    -> */ "@",
    /* HvaultGeomSame      -> */ "~=",
    /* HvaultGeomOverleft  -> */ "&<",
    /* HvaultGeomOverright -> */ "&>",
    /* HvaultGeomOverabove -> */ "|&>",
    /* HvaultGeomOverbelow -> */ "&<|",
    /* HvaultGeomLeft      -> */ "<<",
    /* HvaultGeomRight     -> */ ">>",
    /* HvaultGeomAbove     -> */ "|>>",
    /* HvaultGeomBelow     -> */ "<<|",
    /* HvaultGeomCommLeft  -> */ "&<",
    /* HvaultGeomCommRight -> */ "&>",
    /* HvaultGeomCommAbove -> */ "|&>",
    /* HvaultGeomCommBelow -> */ "&<|",
};

static HvaultGeomOperator geomopcomm[HvaultGeomNumAllOpers] = 
{
    /* HvaultGeomOverlaps  -> */ HvaultGeomOverlaps,
    /* HvaultGeomContains  -> */ HvaultGeomWithin,
    /* HvaultGeomWithin    -> */ HvaultGeomContains,
    /* HvaultGeomSame      -> */ HvaultGeomSame,
    /* HvaultGeomOverleft  -> */ HvaultGeomCommLeft,
    /* HvaultGeomOverright -> */ HvaultGeomCommRight,
    /* HvaultGeomOverabove -> */ HvaultGeomCommAbove,
    /* HvaultGeomOverbelow -> */ HvaultGeomCommBelow,
    /* HvaultGeomLeft      -> */ HvaultGeomRight,
    /* HvaultGeomRight     -> */ HvaultGeomLeft,
    /* HvaultGeomAbove     -> */ HvaultGeomBelow,
    /* HvaultGeomBelow     -> */ HvaultGeomAbove,
    /* HvaultGeomCommLeft  -> */ HvaultGeomOverleft,
    /* HvaultGeomCommRight -> */ HvaultGeomOverright,
    /* HvaultGeomCommAbove -> */ HvaultGeomOverabove,
    /* HvaultGeomCommBelow -> */ HvaultGeomOverbelow,
};

typedef struct {
    HvaultGeomOperator op;
    bool isneg;
} GeomPredicateDesc;

static GeomPredicateDesc geomopmap[2*HvaultGeomNumAllOpers] = 
{
    /* HvaultGeomOverlaps  -> */ { HvaultGeomOverlaps,  false },
    /* HvaultGeomContains  -> */ { HvaultGeomContains,  false },
    /* HvaultGeomWithin    -> */ { HvaultGeomOverlaps,  false },
    /* HvaultGeomSame      -> */ { HvaultGeomContains,  false },
    /* HvaultGeomOverleft  -> */ { HvaultGeomRight,     true  },
    /* HvaultGeomOverright -> */ { HvaultGeomLeft,      true  },
    /* HvaultGeomOverabove -> */ { HvaultGeomBelow,     true  },
    /* HvaultGeomOverbelow -> */ { HvaultGeomAbove,     true  },
    /* HvaultGeomLeft      -> */ { HvaultGeomOverright, true  },
    /* HvaultGeomRight     -> */ { HvaultGeomOverleft,  true  },
    /* HvaultGeomAbove     -> */ { HvaultGeomOverbelow, true  },
    /* HvaultGeomBelow     -> */ { HvaultGeomOverabove, true  },
    /* HvaultGeomCommLeft  -> */ { HvaultGeomCommLeft,  false },
    /* HvaultGeomCommRight -> */ { HvaultGeomCommRight, false },
    /* HvaultGeomCommAbove -> */ { HvaultGeomCommAbove, false },
    /* HvaultGeomCommBelow -> */ { HvaultGeomCommBelow, false },

    /* Negative predicate map */

    /* HvaultGeomOverlaps  -> */ { HvaultGeomWithin,    true  },
    /* HvaultGeomContains  -> */ { HvaultGeomInvalidOp, false },
    /* HvaultGeomWithin    -> */ { HvaultGeomWithin,    true  },
    /* HvaultGeomSame      -> */ { HvaultGeomInvalidOp, false },
    /* HvaultGeomOverleft  -> */ { HvaultGeomOverleft,  true  },
    /* HvaultGeomOverright -> */ { HvaultGeomOverright, true  },
    /* HvaultGeomOverabove -> */ { HvaultGeomOverabove, true  },
    /* HvaultGeomOverbelow -> */ { HvaultGeomOverbelow, true  },
    /* HvaultGeomLeft      -> */ { HvaultGeomLeft,      true  },
    /* HvaultGeomRight     -> */ { HvaultGeomRight,     true  },
    /* HvaultGeomAbove     -> */ { HvaultGeomAbove,     true  },
    /* HvaultGeomBelow     -> */ { HvaultGeomBelow,     true  },
    /* HvaultGeomCommLeft  -> */ { HvaultGeomInvalidOp, false },
    /* HvaultGeomCommRight -> */ { HvaultGeomInvalidOp, false },
    /* HvaultGeomCommAbove -> */ { HvaultGeomInvalidOp, false },
    /* HvaultGeomCommBelow -> */ { HvaultGeomInvalidOp, false },
};

static inline GeomPredicateDesc
mapGeomPredicate(GeomPredicateDesc const p)
{
    size_t idx = p.op;
    if (p.isneg)
        idx += HvaultGeomNumAllOpers;
    return geomopmap[idx];
}

struct HvaultQualAnalyzerData {
    HvaultTableInfo const * table;
    Oid geomopers[HvaultGeomNumRealOpers];
};

struct HvaultQualSimpleData {
    HvaultQual qual;
};

struct HvaultQualGeomData {
    HvaultQual qual;
    Var *var;
    Expr *arg;
    HvaultColumnType coltype;
    GeomPredicateDesc pred;
    GeomPredicateDesc catalog_pred;
};

static inline bool 
isCatalogVar (HvaultColumnType type)
{
    return type == HvaultColumnFileIdx || type == HvaultColumnTime;
}

static bool 
isCatalogQualWalker (Node *node, HvaultTableInfo const *ctx)
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
                if (var->varno == ctx->relid)
                {
                    if (!isCatalogVar(ctx->coltypes[var->varattno-1]))
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

static inline bool 
isCatalogQual (Expr *expr, HvaultTableInfo const *table)
{
    return !isCatalogQualWalker((Node *) expr, (void *) table);
}

/* Catalog only EC will be put into baserestrictinfo by planner, so here
 * we need to extract only ECs that contain both catalog & outer table vars.
 * We skip patalogic case when one EC contains two different catalog vars
 */
static Var *
isCatalogJoinEC (EquivalenceClass *ec, HvaultTableInfo const *table)
{
    ListCell *l;
    Var *catalog_var = NULL;
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
                    if (catalog_var == NULL)
                    {
                        catalog_var = var;
                    }
                    else
                    {
                        return NULL;
                    }
                }
                else
                {
                    return NULL;
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
    return num_outer_vars > 0 && catalog_var ? catalog_var : NULL;
}

static Oid
getGeometryOpOid (char const *opname, SPIPlanPtr prep_stmt)
{
    Datum param[1];
    Datum val;
    bool isnull;

    param[0] = CStringGetTextDatum(opname);
    if (SPI_execute_plan(prep_stmt, param, " ", true, 1) != SPI_OK_SELECT ||
        SPI_processed != 1 || 
        SPI_tuptable->tupdesc->natts != 1 ||
        SPI_tuptable->tupdesc->attrs[0]->atttypid != OIDOID)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't find geometry operator %s", opname)));
        return InvalidOid; /* Will never reach this */
    }

    val = heap_getattr(SPI_tuptable->vals[0], 1, 
                       SPI_tuptable->tupdesc, &isnull);
    if (isnull)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't find geometry operator %s", opname)));
        return InvalidOid; /* Will never reach this */            
    } 
    return DatumGetObjectId(val);
}

#define GEOMETRY_OP_QUERY \
    "SELECT o.oid from pg_opclass c" \
        " JOIN pg_amop ao ON c.opcfamily = ao.amopfamily" \
        " JOIN pg_operator o ON ao.amopopr = o.oid " \
        " WHERE c.opcname = 'gist_geometry_ops_2d' AND o.oprname = $1"

static void
getGeometryOpers (HvaultQualAnalyzer analyzer)
{
    SPIPlanPtr prep_stmt;
    Oid argtypes[1];
    int i;

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                     errmsg("Can't connect to SPI")));
        return; /* Will never reach this */
    }
    
    argtypes[0] = TEXTOID;
    prep_stmt = SPI_prepare(GEOMETRY_OP_QUERY, 1, argtypes);
    if (!prep_stmt)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't prepare geometry operator query")));
        return; /* Will never reach this */
    }

    for (i = 0; i < HvaultGeomNumRealOpers; i++)
    {
        analyzer->geomopers[i] = getGeometryOpOid(hvaultGeomopstr[i], 
                                                  prep_stmt);
    }
    
    if (SPI_finish() != SPI_OK_FINISH)    
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't finish access to SPI")));
        return; /* Will never reach this */   
    }
}

static inline HvaultGeomOperator
getGeometryOper (HvaultQualAnalyzer analyzer, Oid opno)
{
    int i;
    for(i = 0; i < HvaultGeomNumRealOpers; ++i)
        if (analyzer->geomopers[i] == opno) 
            return i;
    return HvaultGeomInvalidOp;
}

static bool
isFootprintOpArgs (Expr *varnode, 
                   Expr *arg, 
                   const HvaultTableInfo *table, 
                   HvaultColumnType *coltype)
{
    Var *var;

    if (!IsA(varnode, Var))
        return false;

    var = (Var *) varnode;
    if (var->varno != table->relid)
        return false;

    *coltype = table->coltypes[var->varattno - 1];
    if (*coltype != HvaultColumnPoint && *coltype != HvaultColumnFootprint)
        return false;

    return true;
}

static bool
isFootprintOp (Expr *expr, 
               HvaultQualAnalyzer analyzer, 
               struct HvaultQualGeomData *qual)
{
    OpExpr *opexpr;
    Expr *first, *second;

    if (!IsA(expr, OpExpr))
        return false;

    opexpr = (OpExpr *) expr;

    if (list_length(opexpr->args) != 2)
        return false;

    qual->pred.op = getGeometryOper(analyzer, opexpr->opno);
    if (qual->pred.op == HvaultGeomInvalidOp)
        return false;

    first = linitial(opexpr->args);
    second = lsecond(opexpr->args);
    if (isFootprintOpArgs(first, second, analyzer->table, &qual->coltype))
    {
        qual->var = (Var *) first;
        qual->arg = second;
    }
    else if (isFootprintOpArgs(second, first, analyzer->table, &qual->coltype))
    {
        qual->var = (Var *) second;
        qual->arg = first;
        qual->pred.op = geomopcomm[qual->pred.op];
    }
    else 
    {
        /* We support only simple var = const expressions */
        return false;
    }

    if (!isCatalogQual(qual->arg, analyzer->table))
        return false;

    
    return true;
}

static bool 
isFootprintNegOp (Expr *expr, 
                  HvaultQualAnalyzer analyzer, 
                  struct HvaultQualGeomData *qual)
{
    BoolExpr *boolexpr;

    if (!IsA(expr, BoolExpr))
        return false;

    boolexpr = (BoolExpr *) expr;

    if (boolexpr->boolop != NOT_EXPR)
        return false;

    if (list_length(boolexpr->args) != 1)
        return false;

    if (!isFootprintOp(linitial(boolexpr->args), analyzer, qual))
        return false;

    return true;
}

static bool
isFootprintQual (Expr *expr, 
                 HvaultQualAnalyzer analyzer, 
                 struct HvaultQualGeomData *qual)
{
    bool res = false;
    if (isFootprintOp(expr, analyzer, qual)) 
    {
        qual->pred.isneg = false;
        res = true;
    }

    if (isFootprintNegOp(expr, analyzer, qual)) 
    {
        qual->pred.isneg = true;
        res = true;
    }

    if (res)
    {
        qual->catalog_pred = mapGeomPredicate(qual->pred);
        return true;
    }
    else 
    {
        return false;
    }
}

/* Initialize analyzer. Reads geometry oper oids */
HvaultQualAnalyzer 
hvaultAnalyzerInit (HvaultTableInfo const * table)
{
    HvaultQualAnalyzer analyzer = palloc(sizeof(struct HvaultQualAnalyzerData));
    analyzer->table = table;
    getGeometryOpers(analyzer);
    return analyzer;
}

/* Free analyer resources */
void 
hvaultAnalyzerFree (HvaultQualAnalyzer analyzer)
{
    pfree(analyzer);
}

/* Process list of restrict infos and 
   return list of interesting HvaultQuals */
List * 
hvaultAnalyzeQuals (HvaultQualAnalyzer analyzer, List * quals)
{
    List * res = NIL;
    ListCell *l;

    foreach(l, quals)
    {
        RestrictInfo *rinfo = lfirst(l);
        struct HvaultQualGeomData geom_qual_data;
        if (isCatalogQual(rinfo->clause, analyzer->table))
        {
            struct HvaultQualSimpleData * qual_data = NULL;
            
            elog(DEBUG2, "Detected catalog qual %s", 
                 nodeToString(rinfo->clause));
            
            qual_data = palloc(sizeof(struct HvaultQualSimpleData));
            qual_data->qual.type = HvaultQualSimple;
            qual_data->qual.rinfo = rinfo;

            res = lappend(res, qual_data);
        } 
        else if (isFootprintQual(rinfo->clause, analyzer, &geom_qual_data))
        {
            struct HvaultQualGeomData * qual_data = NULL;
            
            elog(DEBUG2, "Detected footprint qual %s", 
                 nodeToString(rinfo->clause));
            
            qual_data = palloc(sizeof(struct HvaultQualGeomData));
            memcpy(qual_data, &geom_qual_data, 
                   sizeof(struct HvaultQualGeomData));
            qual_data->qual.type = HvaultQualGeom;
            qual_data->qual.rinfo = rinfo;

            res = lappend(res, qual_data);
        }
    }
    return res;
}

/* Process list of equivalence classes and 
   return list of interesting HvaultECs */
List * 
hvaultAnalyzeECs (HvaultQualAnalyzer analyzer, List * ecs)
{
    List *res = NIL;
    ListCell *l;

    foreach(l, ecs)
    {
        EquivalenceClass *ec = (EquivalenceClass *) lfirst(l);
        Var *catalog_var = isCatalogJoinEC(ec, analyzer->table);
        if (catalog_var != NULL)
        {
            HvaultEC * ec_data = palloc(sizeof(HvaultEC));
            ec_data->ec = ec;
            ec_data->var = catalog_var;

            res = lappend(res, ec_data);
        }
    }
    return res;
}

/* Create predicate data represented as List from qual if possible
   Returns NIL if predicate is not available for this qual */
List * 
hvaultCreatePredicate (HvaultQual * qual, List ** fdw_expr)
{
    struct HvaultQualGeomData * geom_qual = NULL;
    int argno;

    if (qual->type != HvaultQualGeom)
        return NIL;

    geom_qual = (struct HvaultQualGeomData *) qual;
    argno = list_append_unique_pos(fdw_expr, geom_qual->arg);
    return list_make4_int(geom_qual->coltype, geom_qual->pred.op, argno, 
                          geom_qual->pred.isneg);
}

void 
hvaultDeparseQual (HvaultQual * qual, HvaultDeparseContext * ctx)
{
    switch (qual->type) 
    {
        case HvaultQualSimple:
        {
            hvaultDeparseSimple(qual->rinfo->clause, ctx);
        }
        break;
        case HvaultQualGeom:
        {
            struct HvaultQualGeomData * qual_data = 
                (struct HvaultQualGeomData *) qual;
            hvaultDeparseFootprint(qual_data->catalog_pred.op, 
                                   qual_data->catalog_pred.isneg, 
                                   qual_data->arg, 
                                   ctx);
        }
        break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                    errmsg("undefined HvaultQual type")));
    }
}
