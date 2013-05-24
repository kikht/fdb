
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
