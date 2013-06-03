
 Datum val;
        bool isnull;
        char *filename;

        SPI_cursor_fetch(file_cursor, true, 1);
        if (SPI_processed != 1 || SPI_tuptable == NULL)
        {
            /* Can't fetch more files */
            elog(DEBUG1, "No more files");
            break;
        }   

        if (SPI_tuptable->tupdesc->natts != 3 ||
            SPI_tuptable->tupdesc->attrs[0]->atttypid != INT4OID ||
            SPI_tuptable->tupdesc->attrs[1]->atttypid != TEXTOID ||
            SPI_tuptable->tupdesc->attrs[2]->atttypid != TIMESTAMPOID)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Error in catalog query")));
            return false; /* Will never reach this */         
        }

        val = heap_getattr(SPI_tuptable->vals[0], 1, 
                           SPI_tuptable->tupdesc, &isnull);
        if (isnull)
        {
            elog(WARNING, "Wrong entry in catalog, skipping");
            continue;
        }
        scan->cur_file = DatumGetInt32(val);
        filename = SPI_getvalue(SPI_tuptable->vals[0], 
                                SPI_tuptable->tupdesc, 2);
        if (!filename)
        {
            elog(WARNING, "Wrong entry in catalog, skipping");
            continue;
        }
        val = heap_getattr(SPI_tuptable->vals[0], 3, 
                           SPI_tuptable->tupdesc, &isnull);
        scan->file_time = isnull ? 0 : DatumGetTimestamp(val);



static HvaultCatalogCursor *
makeCatalogCursor(char const *query, List *fdw_expr)
{
    HvaultCatalogCursor *cursor;
    ListCell *l;
    int pos, nargs;

    cursor = palloc(sizeof(HvaultCatalogCursor));

    cursor->query = query;
    cursor->file_cursor_name = NULL;
    cursor->prep_stmt = NULL;
    cursor->argtypes = NULL;
    cursor->argvals = NULL;
    cursor->argnulls = NULL;

    pos = 0;
    nargs = list_length(fdw_expr);
    cursor->argtypes = palloc(sizeof(Oid) * nargs);
    cursor->argvals = palloc(sizeof(Datum) * nargs);
    cursor->argnulls = palloc(sizeof(char) * nargs);
    foreach(l, fdw_expr)
    {
        Expr *expr = (Expr *) lfirst(l);
        cursor->argtypes[pos] = exprType((Node *) expr);
        pos++;
    }

    return cursor;
}


static void
addDeparseItem(HvaultDeparseContext *ctx)
{
    if (ctx->first_qual)
    {
        appendStringInfoString(&ctx->query, "WHERE ");
        ctx->first_qual = false;
    }
    else 
    {
        appendStringInfoString(&ctx->query, " AND ");
    }
}



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


foreach(l, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = lfirst(l);
        HvaultQual * qual = hvaultAnalyzerProcess(analyzer, rinfo);
        if (qual)
        {
            *static_quals = lappend(*static_quals, qual);
        }
    }



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



static List *
predicateToList(HvaultGeomPredicate *p)
{
    return list_make4_int(p->coltype, p->op, p->argno, p->isneg);
}


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
