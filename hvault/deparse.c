#include "deparse.h"
#include "utils.h"

/* 
 * Deparse expression as runtime computable parameter. 
 * Parameter index directly maps to fdw_expr.
 */
static void 
deparseParameter(Expr *node, HvaultDeparseContext *ctx)
{
    int idx = list_append_unique_pos(&ctx->fdw_expr, node);
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
        Assert(node->varattno <= ctx->table->natts);

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
        hvaultDeparseSimple((Expr *) linitial(node->args), ctx);
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

        hvaultDeparseSimple((Expr *) linitial(node->args), ctx);
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
        hvaultDeparseSimple((Expr *) lfirst(arg), ctx);
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
        hvaultDeparseSimple(lfirst(arg), ctx);
        appendStringInfoChar(&ctx->query, ' ');
    }

    /* Deparse operator name. */
    deparseOperatorName(&ctx->query, form);

    /* Deparse right operand. */
    if (oprkind == 'l' || oprkind == 'b')
    {
        arg = list_tail(node->args);
        appendStringInfoChar(&ctx->query, ' ');
        hvaultDeparseSimple(lfirst(arg), ctx);
    }

    appendStringInfoChar(&ctx->query, ')');

    ReleaseSysCache(tuple);
}

static void
deparseDistinctExpr(DistinctExpr *node, HvaultDeparseContext *ctx)
{
    Assert(list_length(node->args) == 2);

    appendStringInfoChar(&ctx->query, '(');
    hvaultDeparseSimple(linitial(node->args), ctx);
    appendStringInfo(&ctx->query, " IS DISTINCT FROM ");
    hvaultDeparseSimple(lsecond(node->args), ctx);
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
    hvaultDeparseSimple(node->refexpr, ctx);
    appendStringInfoChar(&ctx->query, ')');
     

    /* Deparse subscript expressions. */
    lowlist_item = list_head(node->reflowerindexpr);    /* could be NULL */
    foreach(uplist_item, node->refupperindexpr)
    {
        appendStringInfoChar(&ctx->query, '[');
        if (lowlist_item)
        {
            hvaultDeparseSimple(lfirst(lowlist_item), ctx);
            appendStringInfoChar(&ctx->query, ':');
            lowlist_item = lnext(lowlist_item);
        }
        hvaultDeparseSimple(lfirst(uplist_item), ctx);
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
    hvaultDeparseSimple(linitial(node->args), ctx);
    appendStringInfoChar(&ctx->query, ' ');
    /* Deparse operator name plus decoration. */
    deparseOperatorName(&ctx->query, form);
    appendStringInfo(&ctx->query, " %s (", node->useOr ? "ANY" : "ALL");
    /* Deparse right operand. */
    hvaultDeparseSimple(lsecond(node->args), ctx);
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
        hvaultDeparseSimple(lfirst(l), ctx);
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
    hvaultDeparseSimple(node->arg, ctx);
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
            hvaultDeparseSimple(linitial(node->args), ctx);
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
        hvaultDeparseSimple((Expr *) lfirst(l), ctx);
        first = false;
    }
    appendStringInfoChar(&ctx->query, ')');
}

static void
deparseNullTest(NullTest *node, HvaultDeparseContext *ctx)
{
    appendStringInfoChar(&ctx->query, '(');
    hvaultDeparseSimple(node->arg, ctx);
    if (node->nulltesttype == IS_NULL)
        appendStringInfo(&ctx->query, " IS NULL)");
    else
        appendStringInfo(&ctx->query, " IS NOT NULL)");
}

static void 
deparseConstant(Const *node, HvaultDeparseContext *ctx)
{
    Oid typoutput;
    bool typIsVarlena;
    char *extval, *quoted = NULL;

    if (node->constisnull)
    {
        appendStringInfo(&ctx->query, "NULL");
        appendStringInfo(&ctx->query, "::%s",
                         format_type_with_typemod(node->consttype,
                                                  node->consttypmod));
        return;
    }

    getTypeOutputInfo(node->consttype,
                      &typoutput, &typIsVarlena);
    extval = OidOutputFunctionCall(typoutput, node->constvalue);

    switch (node->consttype)
    {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case OIDOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            {
                /*
                 * No need to quote unless it's a special value such as 'NaN'.
                 * See comments in get_const_expr().
                 */
                if (strspn(extval, "0123456789+-eE.") == strlen(extval))
                {
                    if (extval[0] == '+' || extval[0] == '-')
                        appendStringInfo(&ctx->query, "(%s)", extval);
                    else
                        appendStringInfoString(&ctx->query, extval);
                }
                else
                    appendStringInfo(&ctx->query, "'%s'", extval);
            }
            break;
        case BITOID:
        case VARBITOID:
            appendStringInfo(&ctx->query, "B'%s'", extval);
            break;
        case BOOLOID:
            if (strcmp(extval, "t") == 0)
                appendStringInfoString(&ctx->query, "true");
            else
                appendStringInfoString(&ctx->query, "false");
            break;
        default:
            quoted = quote_literal_cstr(extval);
            appendStringInfoString(&ctx->query, quoted);
            pfree(quoted);
            quoted = NULL;
            break;
    }

    appendStringInfo(&ctx->query, "::%s",
                     format_type_with_typemod(node->consttype,
                                              node->consttypmod));
}

void
hvaultDeparseSimple(Expr *node, HvaultDeparseContext *ctx)
{
    if (node == NULL)
        return;

    switch (nodeTag(node))
    {
        case T_Var:
            deparseVar((Var *) node, ctx);
            break;
        case T_Const:
            deparseConstant((Const *) node, ctx);
            break;
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

void hvaultDeparseFootprint (HvaultGeomOperator     op, 
                             bool                   isneg, 
                             Expr *                 arg, 
                             HvaultDeparseContext * ctx)
{
    Assert(op != HvaultGeomInvalidOp);

    if (isneg)
        appendStringInfoString(&ctx->query, "NOT ");
    
    appendStringInfoChar(&ctx->query, '(');
    if (op < HvaultGeomNumRealOpers) 
    {
        appendStringInfoString(&ctx->query, "footprint ");
        appendStringInfoString(&ctx->query, hvaultGeomopstr[op]);
        appendStringInfoChar(&ctx->query, ' ');
        hvaultDeparseSimple(arg, ctx);
    }
    else
    {
        hvaultDeparseSimple(arg, ctx);
        appendStringInfoChar(&ctx->query, ' ');
        appendStringInfoString(&ctx->query, hvaultGeomopstr[op]);
        appendStringInfoString(&ctx->query, " footprint");
    }
    appendStringInfoChar(&ctx->query, ')');
}

/* Initializes HvaultDeparseContext struct (with previously undefined contents)
 * to describe empty qual list.
 */
void 
hvaultDeparseContextInit (HvaultDeparseContext  * ctx, 
                          HvaultTableInfo const * table)
{
    ctx->fdw_expr = NIL;
    initStringInfo(&ctx->query);
    ctx->table = table;
}

/* Frees all resources allocated by context. It's up to caller to pfree 
 * the struct itself (so it is possible to allocate struct on stack)
 */
void 
hvaultDeparseContextFree (HvaultDeparseContext * ctx)
{
    list_free(ctx->fdw_expr);
    ctx->table = NULL;
    pfree(ctx->query.data);
}


