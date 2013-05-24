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
    return !isCatalogQualWalker((Node *) expr, (CatalogQualsContext *) table);
}

/* Catalog only EC will be put into baserestrictinfo by planner, so here
 * we need to extract only ECs that contain both catalog & outer table vars.
 * We skip patalogic case when one EC contains two different catalog vars
 */
static Var *
isCatalogJoinEC(EquivalenceClass *ec, HvaultTableInfo const *table)
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

/* -----------------------------------
 *
 * Footprint expression functions
 *
 * -----------------------------------
 */

char * geomopstr[HvaultGeomNumAllOpers] = {

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
    return geomopmap[HvaultGeomNumAllOpers * p.isneg + p.op];
}

static Oid
getGeometryOpOid(char const *opname, SPIPlanPtr prep_stmt)
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

static void
getGeometryOpers(HvaultTableInfo *table)
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
        table->geomopers[i] = getGeometryOpOid(geomopstr[i], prep_stmt);
    }
    
    if (SPI_finish() != SPI_OK_FINISH)    
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't finish access to SPI")));
        return; /* Will never reach this */   
    }
}

static inline HvaultGeomOperator
getGeometryOper(Oid opno, const HvaultTableInfo *table)
{
    int i;
    for(i = 0; i < HvaultGeomNumRealOpers; ++i)
        if (table->geomopers[i] == opno) 
            return i;
    return HvaultGeomInvalidOp;
}

static bool
isFootprintOpArgs(Expr *varnode, 
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
isFootprintOp(Expr *expr, 
              const HvaultTableInfo *table, 
              GeomOpQual *qual)
{
    OpExpr *opexpr;
    Expr *first, *second;

    if (!IsA(expr, OpExpr))
        return false;

    opexpr = (OpExpr *) expr;

    if (list_length(opexpr->args) != 2)
        return false;

    qual->pred.op = getGeometryOper(opexpr->opno, table);
    if (qual->pred.op == HvaultGeomInvalidOp)
        return false;

    first = linitial(opexpr->args);
    second = lsecond(opexpr->args);
    if (isFootprintOpArgs(first, second, table, &qual->coltype))
    {
        qual->var = (Var *) first;
        qual->arg = second;
    }
    else if (isFootprintOpArgs(second, first, table, &qual->coltype))
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

    if (!isCatalogQual(qual->arg, table))
        return false;

    
    return true;
}

static bool 
isFootprintNegOp(Expr *expr, 
                 const HvaultTableInfo *table,
                 GeomOpQual *qual)
{
    BoolExpr *boolexpr;

    if (!IsA(expr, BoolExpr))
        return false;

    boolexpr = (BoolExpr *) expr;

    if (boolexpr->boolop != NOT_EXPR)
        return false;

    if (list_length(boolexpr->args) != 1)
        return false;

    if (!isFootprintOp(linitial(boolexpr->args), table, qual))
        return false;

    return true;
}

static bool
isFootprintQual(Expr *expr, const HvaultTableInfo *table, GeomOpQual *qual)
{
    bool res = false;
    if (isFootprintOp(expr, table, qual)) 
    {
        qual->pred.isneg = false;
        res = true;
    }

    if (isFootprintNegOp(expr, table, qual)) 
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

static void
deparseFootprintExpr(GeomOpQual *qual, HvaultDeparseContext *ctx)
{
    Assert(qual->catalog_pred.op != HvaultGeomInvalidOp);

    if (qual->catalog_pred.isneg)
        appendStringInfoString(&ctx->query, "NOT ");
    
    appendStringInfoChar(&ctx->query, '(');
    if (qual->catalog_pred.op < HvaultGeomNumRealOpers) 
    {
        appendStringInfoString(&ctx->query, "footprint ");
        appendStringInfoString(&ctx->query, geomopstr[qual->catalog_pred.op]);
        appendStringInfoChar(&ctx->query, ' ');
        deparseExpr(qual->arg, ctx);
    }
    else
    {
        deparseExpr(qual->arg, ctx);  
        appendStringInfoChar(&ctx->query, ' ');
        appendStringInfoString(&ctx->query, geomopstr[qual->catalog_pred.op]);
        appendStringInfoString(&ctx->query, " footprint");
    }
    appendStringInfoChar(&ctx->query, ')');
}

