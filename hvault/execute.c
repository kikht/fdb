#include "common.h"
#include "catalog.h"
#include "driver.h"
#include "predicates.h"
#include "options.h"

typedef struct 
{
    HvaultPredicate pred;
    AttrNumber argno;
} Predicate;

typedef struct
{
    char const * cat_name;
    Oid typid;
    AttrNumber attno;
} CatalogColumn;

typedef struct 
{
    MemoryContext memctx;
    List *fdw_expr;      /* List of prepared for computation query expressions*/
    ExprContext *expr_ctx; /* Context for prepared expressions */

    HvaultCatalogCursor cursor;
    HvaultFileDriver * driver;
    HvaultGeolocationType geotype;
    HvaultFileChunk chunk;
    AttrNumber col_indices[HvaultColumnNumTypes];
    List * catalog_columns;

    Predicate * predicates; /* NULL-terminated array of Predicates */
    size_t * sel;
    size_t sel_size, sel_bufsize, cur_pos, chunk_start;

    size_t nattr;

    /* tuple values */
    Datum *values;       /* Tuple values */
    bool *nulls;         /* Tuple null flags */
    LWPOINT *point;      /* Pixel point value */
    LWPOLY *poly;        /* Pixel footprint value */
    POINTARRAY *ptarray; /* Point array for footprint */
} ExecState;

static ExecState * 
makeExecState ()
{
    int i;
    ExecState * state;

    state = palloc0(sizeof(ExecState));

    for (i = 0; i < HvaultColumnNumTypes; i++)
    {
        state->col_indices[i] = -1;
    }

    state->point = lwpoint_make2d(SRID_UNKNOWN, 0, 0);
    state->ptarray = ptarray_construct(false, false, 5);
    state->poly = lwpoly_construct(SRID_UNKNOWN, NULL, 1, &state->ptarray);
    lwgeom_add_bbox(lwpoly_as_lwgeom(state->poly));

    return state;
}

static void 
addCatalogColumn (ExecState * state, Relation rel, int i) 
{
    List * options;
    CatalogColumn * coldata;
    DefElem * name;

    options = GetForeignColumnOptions(RelationGetRelid(rel), i + 1);
    name = defFindByName(options, HVAULT_COLUMN_OPTION_CATNAME);
    if (name == NULL) 
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                errmsg("Catalog column %d has no 'name' option", i)));
    }

    coldata = palloc(sizeof(CatalogColumn));
    coldata->attno = i;
    coldata->cat_name = defGetString(name);
    coldata->typid = RelationGetDescr(rel)->attrs[i]->atttypid;
    state->catalog_columns = lappend(state->catalog_columns, coldata);
}

void 
hvaultBegin (ForeignScanState * node, int eflags)
{
    ExecState * state;
    ForeignScan * const plan = (ForeignScan *) node->ss.ps.plan;
    Relation const rel = node->ss.ss_currentRelation;
    ListCell *l;
    List *packed_query, *packed_predicates, *coltypes;
    int i;
    Oid foreigntableid;
    ForeignTable *foreigntable;

    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    state = makeExecState();
    state->memctx = AllocSetContextCreate(node->ss.ps.state->es_query_cxt, 
                                          "hvault scan context", 
                                          ALLOCSET_DEFAULT_MINSIZE,
                                          ALLOCSET_DEFAULT_INITSIZE,
                                          ALLOCSET_DEFAULT_MAXSIZE);

    state->expr_ctx = node->ss.ps.ps_ExprContext;
    foreach(l, plan->fdw_exprs)
    {
        Expr *expr = (Expr *) lfirst(l);
        state->fdw_expr = lappend(state->fdw_expr, 
                                  ExecInitExpr(expr, &node->ss.ps));
    }

    Assert(list_length(plan->fdw_private) == 3);
    packed_query = linitial(plan->fdw_private);
    packed_predicates = lsecond(plan->fdw_private);
    coltypes = lthird(plan->fdw_private);

    state->nattr = list_length(coltypes);
    state->values = palloc(sizeof(Datum) * state->nattr);
    state->nulls = palloc(sizeof(bool) * state->nattr);

    state->cursor = hvaultCatalogInitCursor(packed_query, state->memctx);
    
    foreigntableid = RelationGetRelid(rel);
    foreigntable = GetForeignTable(foreigntableid);
    state->driver = hvaultGetDriver(foreigntable->options, state->memctx);
    state->geotype = state->driver->geotype;

    i = 0;
    foreach(l, coltypes)
    {
        HvaultColumnType type = lfirst_int(l);
        if (type >= HvaultColumnIndex && type <= HvaultColumnPoint)
        {
            if (state->col_indices[type] != -1)
            {
                /* TODO: better message */
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), 
                                errmsg("Duplicate special column"),
                                errhint("Check hvault table definition")));
            }
            state->col_indices[type] = i;
        }

        if (type == HvaultColumnCatalog)
            addCatalogColumn(state, rel, i);

        if (type >= HvaultColumnFootprint && type <= HvaultColumnDataset)
        {
            Form_pg_attribute attr = RelationGetDescr(rel)->attrs[i];
            state->driver->methods->add_column(state->driver, attr,
                GetForeignColumnOptions(foreigntableid, i+1));
        }

        i++;
    }

    /* Predicate initialization */
    state->predicates = palloc(sizeof(Predicate) * 
                               (list_length(packed_predicates) + 1));
    i = 0;
    foreach(l, packed_predicates)
    {
        List * pred = lfirst(l);
        HvaultColumnType coltype;
        HvaultGeomOperator op;
        AttrNumber argno;
        bool isneg;

        hvaultUnpackPredicate(pred, &coltype, &op, &argno, &isneg);
        state->predicates[i].argno = argno;
        state->predicates[i].pred = hvaultGetPredicate(op, isneg, coltype, 
                                                       state->geotype);
        if (pred == NULL)
        {
            elog(ERROR, "Unknown predicate type: %d %d %d %d", op, isneg, 
                 coltype, state->geotype);
            return; /* Will never reach this */
        }
        i++;
    }
    state->predicates[i].pred = NULL;

    node->fdw_state = state;
}

void 
hvaultReScan(ForeignScanState *node)
{
    ExecState *state = (ExecState *) node->fdw_state;

    elog(DEBUG1, "in hvaultReScan");
    if (state == NULL)
        return;

    if (state->driver)
        state->driver->methods->close(state->driver);

    if (state->cursor)
        hvaultCatlogResetCursor(state->cursor);

    state->sel_size = 0;
    state->cur_pos = 0;
}

void 
hvaultEnd(ForeignScanState *node)
{
    ExecState *state = (ExecState *) node->fdw_state;
    
    elog(DEBUG1, "in hvaultEnd");
    if (state == NULL)
        return;

    if (state->driver) 
        state->driver->methods->free(state->driver);

    if (state->cursor)
        hvaultCatalogFreeCursor(state->cursor);

    MemoryContextDelete(state->memctx);
}

static bool 
fetchNextFile (ExecState *state)
{
    HvaultCatalogCursorResult res;
    HvaultCatalogItem const * products;

    Assert(state->cursor);
    res = hvaultCatalogNext(state->cursor);
    if (res == HvaultCatalogCursorNotStarted)
    {
        int nargs, pos;
        Oid * argtypes;
        Datum * argvals;
        char * argnulls;
        ListCell * fdw_expr;

        nargs = hvaultCatalogGetNumArgs(state->cursor);
        argtypes = palloc(nargs * sizeof(Oid));
        argvals = palloc(nargs * sizeof(Datum));
        argnulls = palloc(nargs * sizeof(char));

        Assert(list_length(state->fdw_expr) >= nargs);
        fdw_expr = list_head(state->fdw_expr);
        for (pos = 0; pos < nargs; pos++, fdw_expr = lnext(fdw_expr)) 
        {
            ExprState *expr;
            bool isnull;   

            Assert(fdw_expr);
            expr = (ExprState *) lfirst(fdw_expr);
            Assert(IsA(expr, ExprState));
            argvals[pos] = ExecEvalExpr(expr, state->expr_ctx, &isnull, NULL);
            argnulls[pos] = isnull ? 'n' : ' ';
            argtypes[pos] = exprType((Node *) expr->expr);
        }

        hvaultCatalogStartCursor(state->cursor, argtypes, argvals, argnulls);

        pfree(argtypes);
        pfree(argvals);
        pfree(argnulls);

        res = hvaultCatalogNext(state->cursor);
    }

    switch (res) 
    {
        case HvaultCatalogCursorEOF:
            /* Can't fetch more files */
            return false;
        case HvaultCatalogCursorOK:
            /* nop */
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Unexpected cursor retval %d", res)));
            return false; /* Will never reach this */                
    }
    products = hvaultCatalogGetValues(state->cursor);
    state->driver->methods->open(state->driver, products);
    state->chunk_start = 0;
    return true;
}

static void 
fillAllColumnsWithNull (ExecState *state)
{
    size_t i;
    for (i = 0; i < state->nattr; i++)
        state->nulls[i] = true;
}

static void
fillCatalogColumns (ExecState *state)
{
    ListCell * l;
    HvaultCatalogItem const * cat_row = hvaultCatalogGetValues(state->cursor);
    foreach(l, state->catalog_columns)
    {
        CatalogColumn * col = lfirst(l);
        HvaultCatalogItem * item = NULL;

        HASH_FIND_STR(cat_row, col->cat_name, item);
        if (item == NULL) 
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't find catalog column value: %s", 
                                   col->cat_name),
                            errhint("Check table and catalog definition")));
            return; /* Will never reach this */
        }

        if (col->typid != item->typid)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Incompatible catalog column type for %s", 
                                   col->cat_name),
                            errhint("Check table and catalog definition")));
            return; /* Will never reach this */
        }

        state->nulls[col->attno] = item->str == NULL;
        state->values[col->attno] = item->val;
    }
}

static inline bool 
nextChunkNeeded (ExecState *state)
{
    return state->cur_pos == state->sel_size;
}

static bool
fetchNextChunk (ExecState *state)
{
    state->chunk_start += state->chunk.size;
    state->driver->methods->read(state->driver, &state->chunk);
    state->sel_size = state->chunk.size;
    state->cur_pos = 0;
    return state->chunk.size != 0;
}

static void
calculatePredicates (ExecState *state)
{
    Predicate *pred;
    /*Allocate buffer if necessary */
    if (state->sel_bufsize < state->chunk.size) 
    {
        MemoryContext oldmemctx = MemoryContextSwitchTo(state->memctx);
        if (state->sel != NULL)
            pfree(state->sel);
        state->sel = palloc(state->chunk.size * sizeof(size_t));
        MemoryContextSwitchTo(oldmemctx);
    }

    state->sel_size = state->chunk.size;
    /*Call predicates one by one */
    pred = state->predicates;
    while (pred->pred != NULL)
    {
        ExprState *expr;
        bool isnull;
        Datum argdatum;
        GSERIALIZED * arggeom;
        GBOX arg;

        expr = list_nth(state->fdw_expr, pred->argno);
        argdatum = ExecEvalExpr(expr, state->expr_ctx, &isnull, NULL);
        if (isnull) 
        {
            state->sel_size = 0;
            break;
        }
        arggeom = (GSERIALIZED*) PG_DETOAST_DATUM(argdatum);
        if (gserialized_get_gbox_p(arggeom, &arg) == LW_FAILURE)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't get GBOX from predicate arg")));
            return; /* Will never reach here */
        }

        state->sel_size = pred->pred(state->sel, state->sel_size, 
                                     &state->chunk, &arg);
        if (state->sel_size == 0)
            break;

        pred++;
    }
}

static inline void 
fillOneColumn (ExecState * state, HvaultFileLayer const * layer, size_t idx)
{
    if (layer->fill_val != NULL && 
        !memcmp(layer->data, layer->fill_val, layer->item_size))
    {
        state->nulls[layer->colnum] = true;
        return;
    }

#define typedFill(type, datumConverter) \
{ \
    type val = (((type *) layer->data)[idx]); \
    state->nulls[layer->colnum] = false; \
    if (layer->scale == 0) \
    { \
        state->values[layer->colnum] = datumConverter(val); \
    } \
    else \
    { \
        double * const static_dst = layer->temp; \
        double dst = val; \
        if (layer->range != NULL) \
        { \
            type lower = (((type *) layer->range)[0]); \
            type upper = (((type *) layer->range)[1]); \
            if (val < lower || val > upper) \
            { \
                state->nulls[layer->colnum] = true; \
                return; \
            } \
        } \
        *static_dst = layer->scale * (dst - layer->offset); \
        state->values[layer->colnum] = Float8GetDatumFast(*static_dst); \
    } \
} while(0)

    switch (layer->src_type)
    {
        case HvaultInt8:
            typedFill(int8_t, Int8GetDatum);
            break;
        case HvaultUInt8:
            typedFill(uint8_t, UInt8GetDatum);
            break;
        case HvaultInt16:
            typedFill(int16_t, Int16GetDatum);
            break;
        case HvaultUInt16:
            typedFill(uint16_t, UInt16GetDatum);
            break;
        case HvaultInt32:
            typedFill(int32_t, Int32GetDatum);
            break;
        case HvaultUInt32:
            typedFill(int32_t, UInt32GetDatum);
            break;
        case HvaultInt64:
            typedFill(int64_t, Int64GetDatumFast);
            break;
        case HvaultUInt64:
            typedFill(uint64_t, Int64GetDatumFast);
            break;
        case HvaultFloat32:
            typedFill(float, Float4GetDatum);
            break;
        case HvaultFloat64:
            typedFill(double, Float8GetDatumFast);
            break;
        case HvaultBitmap:
            {
                memcpy(VARBITS(layer->temp), 
                       ((char *) layer->data) + layer->item_size * idx, 
                       layer->item_size);
                state->values[layer->colnum] = VarBitPGetDatum(layer->temp);
                state->nulls[layer->colnum] = false;
            }
            break;
        case HvaultPrefixBitmap:
            {
                size_t i;
                const size_t bitmap_stride = state->chunk.size 
                    / layer->hfactor / layer->vfactor;
                for (i = 0; i < layer->item_size; i++)
                {
                    VARBITS(layer->temp)[i] = 
                        ((char *) layer->data)[idx + i * bitmap_stride];
                }
                state->values[layer->colnum] = VarBitPGetDatum(layer->temp);
                state->nulls[layer->colnum] = false;
            }
            break;
        default:
            elog(ERROR, "Datatype is not supported");
            return; /* Will never reach this */
    }

#undef typedFill
}

static void
fillChunkColumns (ExecState *state)
{
    /* Calculate const dataset values */
    ListCell * l;
    foreach(l, state->chunk.const_layers)
    {
        HvaultFileLayer * layer = lfirst(l);
        Assert(layer->type == HvaultLayerConst);
        fillOneColumn(state, layer, 0);
    }
}

static void 
fillPixelColumns (ExecState *state)
{
    ListCell *l;
    size_t cur_idx;

    if (state->sel_size != state->chunk.size)
        cur_idx = state->sel[state->cur_pos];
    else 
        cur_idx = state->cur_pos;

    if (state->col_indices[HvaultColumnIndex] >= 0)
    {
        state->nulls[state->col_indices[HvaultColumnIndex]] = false;
        state->values[state->col_indices[HvaultColumnIndex]] = 
            state->chunk_start + cur_idx;
    }

    if (state->col_indices[HvaultColumnLineIdx] >= 0)
    {
        state->nulls[state->col_indices[HvaultColumnLineIdx]] = false;
        state->values[state->col_indices[HvaultColumnLineIdx]] = 
            (state->chunk_start + cur_idx) / state->chunk.stride;
    }

    if (state->col_indices[HvaultColumnSampleIdx] >= 0)
    {
        state->nulls[state->col_indices[HvaultColumnSampleIdx]] = false;
        state->values[state->col_indices[HvaultColumnSampleIdx]] = 
            cur_idx % state->chunk.stride;
    }

    if (state->col_indices[HvaultColumnFootprint] >= 0)
    {
        GSERIALIZED *ret;
        /* This is quite dirty code that uses internal representation of LWPOLY.
           However it is a very hot place here */
        double *data = (double *) state->poly->rings[0]->serialized_pointlist;
        switch (state->geotype)
        {
            case HvaultGeolocationSimple:
            {
                float const * cur_lat = state->chunk.lat + cur_idx * 4;
                float const * cur_lon = state->chunk.lon + cur_idx * 4;   
                data[0] = cur_lon[0]; 
                data[1] = cur_lat[0]; 
                data[2] = cur_lon[1]; 
                data[3] = cur_lat[1]; 
                data[4] = cur_lon[2]; 
                data[5] = cur_lat[2]; 
                data[6] = cur_lon[3]; 
                data[7] = cur_lat[3]; 
                data[8] = cur_lon[0]; 
                data[9] = cur_lat[0]; 
            }
            break;
            case HvaultGeolocationCompact:
            {
                size_t const line = state->chunk.stride;
                size_t const idx = cur_idx + cur_idx / line;
                float const * cur_lat = state->chunk.lat + idx;
                float const * cur_lon = state->chunk.lon + idx;
                data[0] = cur_lon[0]; 
                data[1] = cur_lat[0]; 
                data[2] = cur_lon[1]; 
                data[3] = cur_lat[1]; 
                data[4] = cur_lon[state->chunk.stride+2]; 
                data[5] = cur_lat[state->chunk.stride+2]; 
                data[6] = cur_lon[state->chunk.stride+1]; 
                data[7] = cur_lat[state->chunk.stride+1]; 
                data[8] = cur_lon[0]; 
                data[9] = cur_lat[0]; 
            }
            break;
            default:
                elog(ERROR, "Geolocation type is not supported");
        }
        if (data[0] > 360.0 || data[0] < -180.0 ||
            data[2] > 360.0 || data[2] < -180.0 ||
            data[4] > 360.0 || data[4] < -180.0 ||
            data[6] > 360.0 || data[6] < -180.0 ||
            data[1] > 90.0  || data[1] < -90.0  ||
            data[3] > 90.0  || data[3] < -90.0  ||
            data[5] > 90.0  || data[5] < -90.0  ||
            data[7] > 90.0  || data[7] < -90.0  )
        {
            state->nulls[state->col_indices[HvaultColumnFootprint]] = true;
        }
        else
        {
            lwgeom_calculate_gbox((LWGEOM *) state->poly, state->poly->bbox);
            ret = gserialized_from_lwgeom((LWGEOM *) state->poly, true, NULL);
            state->nulls[state->col_indices[HvaultColumnFootprint]] = false;
            state->values[state->col_indices[HvaultColumnFootprint]] = 
                PointerGetDatum(ret);   
        }
    }

    if (state->col_indices[HvaultColumnPoint] >= 0)
    {
        /* This is quite dirty code that uses internal representation of LWPOINT.
           However it is a very hot place here */
        GSERIALIZED *ret;
        double *data = (double *) state->point->point->serialized_pointlist;
        data[0] = state->chunk.point_lon[cur_idx];
        data[1] = state->chunk.point_lat[cur_idx];
        if (data[0] > 360.0 || data[0] < -180.0 ||
            data[1] > 90.0  || data[1] < -90.0)
        {
            state->nulls[state->col_indices[HvaultColumnPoint]] = true;
        }
        else
        {
            ret = gserialized_from_lwgeom((LWGEOM *) state->point, true, NULL);
            state->nulls[state->col_indices[HvaultColumnPoint]] = false;
            state->values[state->col_indices[HvaultColumnPoint]] = 
                PointerGetDatum(ret);
        }
    }

    foreach(l, state->chunk.layers)
    {
        HvaultFileLayer * layer = lfirst(l);
        size_t idx;
        switch (layer->type)
        {
            case HvaultLayerSimple:
                idx = cur_idx;
                break;
            case HvaultLayerChunked:
            {
                size_t line = state->chunk.stride;
                size_t vfactor = layer->vfactor;
                size_t hfactor = layer->hfactor;
                idx = cur_idx;
                idx = ((idx / line) / vfactor * line + idx % line ) / hfactor;
            }
                break;
            default:
                elog(ERROR, "Layer type is not supported");
                return; /* Will never reach this */
        }
        fillOneColumn(state, layer, idx);
    }
}

static void 
incrementPosition (ExecState *state)
{
    state->cur_pos++;
}

TupleTableSlot *
hvaultIterate (ForeignScanState *node) 
{
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    ExecState *state = node->fdw_state;

    ExecClearTuple(slot);

    while (nextChunkNeeded(state))
    {
        while (!fetchNextChunk(state))
        {
            if (fetchNextFile(state))
            {
                fillAllColumnsWithNull(state);
                fillCatalogColumns(state);
            }
            else
            {
                /* End of scan, return empty tuple*/
                elog(DEBUG1, "End of scan: no more files");
                return slot;
            }
        }
        
        calculatePredicates(state);
        if (nextChunkNeeded(state)) 
        {
            /* Continue to next chunk */
            continue;
        }
        fillChunkColumns(state);
    }

    fillPixelColumns(state);
    incrementPosition(state);

    slot->tts_isnull = state->nulls;
    slot->tts_values = state->values;
    ExecStoreVirtualTuple(slot);
    return slot;
}

void 
hvaultExplain(ForeignScanState *node, ExplainState *es)
{
    ForeignScan *plan;
    List *packed_query, *packed_predicates, *coltypes;
    HvaultCatalogCursor cursor;
    List * active_columns;
    char const * query_str;
    ListCell *l;
    int i;
    TupleDesc tupdesc;
    List *pred_str;
    List *dpcontext;

    plan = (ForeignScan *) node->ss.ps.plan;

    Assert(list_length(plan->fdw_private) == 3);
    packed_query = linitial(plan->fdw_private);
    packed_predicates = lsecond(plan->fdw_private);
    coltypes = lthird(plan->fdw_private);

    tupdesc = RelationGetDescr(node->ss.ss_currentRelation);
    active_columns = NIL;
    i = 0;
    foreach(l, coltypes)
    {
        HvaultColumnType coltype = lfirst_int(l);
        if (coltype != HvaultColumnNull)
        {
            active_columns = lappend(active_columns, 
                                     tupdesc->attrs[i]->attname.data);
        }
        i++;
    }
    if (list_length(active_columns) > 0)
        ExplainPropertyList("Active columns", active_columns, es);
    else
        ExplainPropertyText("Active columns", "<none>", es);

    cursor = hvaultCatalogInitCursor(packed_query, CurrentMemoryContext);
    query_str = hvaultCatalogGetQuery(cursor);
    ExplainPropertyText("Catalog query", query_str, es);
    hvaultCatalogFreeCursor(cursor);

    pred_str = NIL;
    foreach(l, packed_predicates)
    {
        List * pred = lfirst(l);
        HvaultColumnType coltype;
        HvaultGeomOperator op;
        AttrNumber argno;
        bool isneg;
        char *colname;
        StringInfoData str;

        initStringInfo(&str);
        hvaultUnpackPredicate(pred, &coltype, &op, &argno, &isneg);
        switch (coltype) {
            case HvaultColumnFootprint:
                colname = "footprint";
                break;
            case HvaultColumnPoint:
                colname = "point";
                break;
            default:
                colname = "<unknown>";
        }

        if (isneg)
            appendStringInfoString(&str, "NOT ");
    
        if (op < HvaultGeomNumRealOpers) 
        {
            appendStringInfo(&str, "%s %s $%d", 
                             colname, hvaultGeomopstr[op], argno+1);
        }
        else
        {
            appendStringInfo(&str, "$%d %s %s", 
                             argno+1, hvaultGeomopstr[op], colname);
        }
        pred_str = lappend(pred_str, str.data);
    }
    
    if (list_length(pred_str) > 0)
        ExplainPropertyList("Geometry predicates", pred_str, es);

    i = 1;
#if PG_VERSION_NUM >= 90300
    dpcontext = deparse_context_for_planstate((Node*) node, 
                                              NIL, 
                                              es->rtable,
                                              es->rtable_names);
#else
    dpcontext = deparse_context_for_planstate((Node*) node, 
                                              NIL, 
                                              es->rtable);
#endif
    foreach(l, plan->fdw_exprs)
    {
        StringInfoData str;
        char *expr;

        initStringInfo(&str);
        appendStringInfo(&str, "arg%d", i);
        expr = deparse_expression(lfirst(l), dpcontext, false, false);
        ExplainPropertyText(str.data, expr, es);
        i++;
    }
}

static int
aquireSampleRows(Relation relation,
                 int elevel,
                 HeapTuple *rows,
                 int targrows,
                 double *totalrows,
                 double *totaldeadrows)
{
    ExecState *state;
    Oid foreigntableid;
    TupleDesc tupdesc;
    int num_rows = 0;
    int num_files;
    double rowstoskip = -1;
    double rstate = anl_init_selection_state(targrows);
    double procrows = 0;
    HvaultCatalogQuery query;
    HvaultTableInfo table;
    ForeignTable *foreigntable;
    size_t i;

    (void)(elevel);

    foreigntableid = RelationGetRelid(relation);
    tupdesc = RelationGetDescr(relation);
    foreigntable = GetForeignTable(foreigntableid);

    table.relid = 0;
    table.natts = 0;
    table.columns = NULL;
    table.catalog = hvaultGetTableOptionString(foreigntableid, "catalog");
    if (table.catalog == NULL)
        return 0;

    num_files = targrows / 100;
    if (num_files < 5)
        num_files = 5;
    query = hvaultCatalogInitQuery(&table);
    hvaultCatalogAddColumn(query, "*");
    hvaultCatalogSetSort(query, "random()", false);
    hvaultCatalogSetLimit(query, num_files);

    state = makeExecState();
    state->memctx = CurrentMemoryContext;
    state->nattr = tupdesc->natts;
    state->values = palloc(sizeof(Datum) * state->nattr);
    state->nulls = palloc(sizeof(bool) * state->nattr);
    state->cursor = hvaultCatalogInitCursor(hvaultCatalogPackQuery(query), 
                                            state->memctx);
    state->driver = hvaultGetDriver(foreigntable->options, state->memctx);
    state->geotype = state->driver->geotype;

    for (i = 0; i < state->nattr; i++)
    {
        List *options = GetForeignColumnOptions(foreigntableid, i+1);
        HvaultColumnType type = hvaultGetColumnType(defFindByName(options, 
                                                                  "type"));
        state->col_indices[type] = i;

        if (type == HvaultColumnCatalog)
            addCatalogColumn(state, relation, i);

        if (type >= HvaultColumnFootprint && type <= HvaultColumnDataset)
        {
            Form_pg_attribute attr = tupdesc->attrs[i];
            state->driver->methods->add_column(state->driver, attr,
                GetForeignColumnOptions(foreigntableid, i+1));
        }
    }

    memset(rows, 0, sizeof(HeapTuple) * targrows);
    while (fetchNextFile(state))
    {
        vacuum_delay_point();
        fillAllColumnsWithNull(state);
        fillCatalogColumns(state);
        while (fetchNextChunk(state))
        {
            vacuum_delay_point();
            fillChunkColumns(state);
            while (!nextChunkNeeded(state))
            {
                int pos = -1;
                procrows += 1;
                if (num_rows >= targrows)
                {
                    if (rowstoskip < 0)
                        rowstoskip = anl_get_next_S(procrows, targrows, 
                                                    &rstate);

                    if (rowstoskip <= 0)
                    {
                        pos = (int) (anl_random_fract() * targrows);
                        heap_freetuple(rows[pos]);
                        rows[pos] = NULL;
                    }
                    else
                    {
                        pos = -1;
                    }
                    rowstoskip -= 1;
                }
                else
                {
                    pos = num_rows;
                    num_rows++;
                }

                if (pos >= 0)
                {
                    fillPixelColumns(state);
                    Assert(rows[pos] == NULL);
                    rows[pos] = heap_form_tuple(tupdesc, state->values, 
                                                state->nulls);
                }
                incrementPosition(state);
            }
        }
    }

    /* TODO: get rid of HVAULT_TUPLES_PER_FILE as it depends on driver */
    *totalrows = hvaultGetNumFiles(table.catalog) * HVAULT_TUPLES_PER_FILE;
    *totaldeadrows = 0;
    return num_rows;
}

bool 
hvaultAnalyze(Relation relation, 
              AcquireSampleRowsFunc *func,
              BlockNumber *totalpages)
{
    char *catalog;
    StringInfo query;
    Datum val;
    bool isnull;
    Oid argtypes[] = {INT4OID};
    Datum argvals[] = {BLCKSZ};


    catalog = hvaultGetTableOptionString(RelationGetRelid(relation), "catalog");
    if (catalog == NULL)
        return false;

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't connect to SPI")));
        return false; /* Will never reach this */
    }

    query = makeStringInfo();
    /* FIXME: this query no longer works */
    appendStringInfo(query, 
                     "SELECT CAST(((SUM(size) + $1 - 1)/$1) AS float8) FROM %s", 
                     catalog);
    if (SPI_execute_with_args(query->data, 1, argtypes, argvals, NULL, true, 1) 
            != SPI_OK_SELECT ||
        SPI_processed != 1 ||
        SPI_tuptable->tupdesc->natts != 1 ||
        SPI_tuptable->tupdesc->attrs[0]->atttypid != FLOAT8OID)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't select from catalog %s", catalog)));
        return false; /* Will never reach this */
    }
    pfree(query->data);
    pfree(query);

    val = heap_getattr(SPI_tuptable->vals[0], 1, 
                       SPI_tuptable->tupdesc, &isnull);
    if (isnull)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't select from catalog %s", catalog)));
        return false; /* Will never reach this */            
    }


    *totalpages = DatumGetFloat8(val);
    if (*totalpages < 1)
        *totalpages = 1;

    if (SPI_finish() != SPI_OK_FINISH)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't finish access to SPI")));
        return false; /* Will never reach this */
    }
    *func = aquireSampleRows;
    return true;
}
