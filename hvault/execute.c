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
} ExecState;

static ExecState * makeExecState ()
{
    int i;
    POINTARRAY * ptarray;
    ExecState * state;

    state = palloc0(sizeof(ExecState));

    for (i = 0; i < HvaultColumnNumTypes; i++)
    {
        state->col_indices[i] = -1;
    }

    state->point = lwpoint_make2d(SRID_UNKNOWN, 0, 0);
    ptarray = ptarray_construct(false, false, 5);
    state->poly = lwpoly_construct(SRID_UNKNOWN, NULL, 1, &ptarray);
    lwgeom_add_bbox(lwpoly_as_lwgeom(state->poly));

    return state;
}

void hvaultBegin (ForeignScanState * node, int eflags)
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
        {
            List * options;
            CatalogColumn * coldata;
            DefElem * name;

            options = GetForeignColumnOptions(foreigntableid, i + 1);
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
        HvaultColumnType coltype = linitial_int(pred);
        HvaultGeomOperator op = lsecond_int(pred);
        AttrNumber argno = lthird_int(pred);
        bool isneg = lfourth_int(pred);

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
            elog(DEBUG1, "No more files");
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
    state->nulls[layer->colnum] = false;

    if (layer->scale == 0) 
    {
        void *src = layer->data;
        Datum dst;

        switch (layer->src_type)
        {
            case HvaultInt8:
                dst = Int8GetDatum      (((int8_t *)   src)[idx]);
                break;
            case HvaultInt16:
                dst = Int16GetDatum     (((int16_t *)  src)[idx]);
                break;
            case HvaultInt32:
                dst = Int32GetDatum     (((int32_t *)  src)[idx]);
                break;
            case HvaultInt64:
                dst = Int64GetDatumFast (((int64_t *)  src)[idx]);
                break;
            case HvaultFloat32:
                dst = Float4GetDatum    (((float *)    src)[idx]);
                break;
            case HvaultFloat64:
                dst = Float8GetDatumFast(((double *)   src)[idx]);
                break;
            case HvaultBitmap:
                {
                    memcpy(VARBITS(layer->temp), 
                           ((char *) src) + layer->item_size * idx, 
                           layer->item_size);
                    dst = VarBitPGetDatum(layer->temp);
                }
                break;
            case HvaultUInt8:
                dst = UInt8GetDatum     (((uint8_t *)  src)[idx]);
                break;
            case HvaultUInt16:
                dst = UInt16GetDatum    (((uint16_t *) src)[idx]);
                break;
            case HvaultUInt32:
                dst = UInt32GetDatum    (((uint32_t *) src)[idx]);
                break;
            case HvaultUInt64:
                dst = Int64GetDatumFast (((uint64_t *) src)[idx]);
                break;
            default:
                elog(ERROR, "Datatype is not supported");
                return; /* Will never reach this */
        }
        state->values[layer->colnum] = dst;
    }
    else 
    {
        void const * const src = layer->data;
        double dst;
        double * const static_dst = layer->temp;
        switch (layer->src_type)
        {
            case HvaultInt8:
                dst = ((int8_t *)   src)[idx];
                break;
            case HvaultInt16:
                dst = ((int16_t *)  src)[idx];
                break;
            case HvaultInt32:
                dst = ((int32_t *)  src)[idx];
                break;
            case HvaultInt64:
                dst = ((int64_t *)  src)[idx];
                break;
            case HvaultFloat32:
                dst = ((float *)    src)[idx];
                break;
            case HvaultFloat64:
                dst = ((double *)   src)[idx];
                break;
            case HvaultBitmap:
                elog(ERROR, "Scaled bitmaps are not supported");
                return; /* Will never reach this */
            case HvaultUInt8:
                dst = ((uint8_t *)  src)[idx];
                break;
            case HvaultUInt16:
                dst = ((uint16_t *) src)[idx];
                break;
            case HvaultUInt32:
                dst = ((uint32_t *) src)[idx];
                break;
            case HvaultUInt64:
                dst = ((uint64_t *) src)[idx];
                break;    
            default:
                elog(ERROR, "Datatype is not supported");
                return; /* Will never reach this */
        }
        *static_dst = dst / layer->scale - layer->offset;
        state->values[layer->colnum] = Float8GetDatumFast(dst);
    }
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

    if (state->col_indices[HvaultColumnIndex] >= 0)
    {
        state->nulls[state->col_indices[HvaultColumnIndex]] = false;
        state->values[state->col_indices[HvaultColumnIndex]] = 
            state->chunk_start + state->cur_pos;
    }

    if (state->col_indices[HvaultColumnLineIdx] >= 0)
    {
        state->nulls[state->col_indices[HvaultColumnLineIdx]] = false;
        state->values[state->col_indices[HvaultColumnLineIdx]] = 
            (state->chunk_start + state->cur_pos) / state->chunk.stride;
    }

    if (state->col_indices[HvaultColumnSampleIdx] >= 0)
    {
        state->nulls[state->col_indices[HvaultColumnSampleIdx]] = false;
        state->values[state->col_indices[HvaultColumnSampleIdx]] = 
            state->cur_pos % state->chunk.stride;
    }

    if (state->col_indices[HvaultColumnFootprint] >= 0)
    {
        GSERIALIZED *ret;
        double *data = (double *) state->poly->rings[0]->serialized_pointlist;
        switch (state->geotype)
        {
            case HvaultGeolocationSimple:
            {
                float const * cur_lat = state->chunk.lat + state->cur_pos * 4;
                float const * cur_lon = state->chunk.lon + state->cur_pos * 4;   
                data[0] = cur_lat[0]; 
                data[1] = cur_lon[0]; 
                data[2] = cur_lat[1]; 
                data[3] = cur_lon[1]; 
                data[4] = cur_lat[2]; 
                data[5] = cur_lon[2]; 
                data[6] = cur_lat[3]; 
                data[7] = cur_lon[3]; 
                data[8] = cur_lat[0]; 
                data[9] = cur_lon[0]; 
            }
            break;
            case HvaultGeolocationCompact:
            {
                float const * cur_lat = state->chunk.lat + state->cur_pos;
                float const * cur_lon = state->chunk.lon + state->cur_pos;
                data[0] = cur_lat[0]; 
                data[1] = cur_lon[0]; 
                data[2] = cur_lat[1]; 
                data[3] = cur_lon[1]; 
                data[4] = cur_lat[state->chunk.stride+1]; 
                data[5] = cur_lon[state->chunk.stride+1]; 
                data[6] = cur_lat[state->chunk.stride]; 
                data[7] = cur_lon[state->chunk.stride]; 
                data[8] = cur_lat[0]; 
                data[9] = cur_lon[0]; 
            }
            break;
            default:
                elog(ERROR, "Geolocation type is not supported");
        }
        /* This is quite dirty code that uses internal representation of LWPOLY.
           However it is a very hot place here */
        lwgeom_calculate_gbox((LWGEOM *) state->poly, state->poly->bbox);
        ret = gserialized_from_lwgeom((LWGEOM *) state->poly, true, NULL);
        state->nulls[state->col_indices[HvaultColumnFootprint]] = false;
        state->values[state->col_indices[HvaultColumnFootprint]] = 
            PointerGetDatum(ret);   
    }

    if (state->col_indices[HvaultColumnPoint] >= 0)
    {
        /* This is quite dirty code that uses internal representation of LWPOINT.
           However it is a very hot place here */
        GSERIALIZED *ret;
        double *data = (double *) state->point->point->serialized_pointlist;
        data[0] = state->chunk.point_lat[state->cur_pos];
        data[1] = state->chunk.point_lon[state->cur_pos];
        ret = gserialized_from_lwgeom((LWGEOM *) state->point, true, NULL);
        state->nulls[state->col_indices[HvaultColumnPoint]] = false;
        state->values[state->col_indices[HvaultColumnPoint]] = 
            PointerGetDatum(ret);
    }

    foreach(l, state->chunk.layers)
    {
        HvaultFileLayer * layer = lfirst(l);
        size_t idx;
        switch (layer->type)
        {
            case HvaultLayerSimple:
                idx = state->cur_pos;
                break;
            case HvaultLayerChunked:
            {
                size_t line = state->chunk.stride;
                size_t vfactor = layer->vfactor;
                size_t hfactor = layer->hfactor;
                idx = state->cur_pos;
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
    //TODO: port from old version

    // ForeignScan *plan;
    // List *fdw_plan_private;
    // Oid foreigntableid;
    // char *catalog;
    // List *predicates, *pred_str;
    // ListCell *l;

    // plan = (ForeignScan *) node->ss.ps.plan;
    // fdw_plan_private = plan->fdw_private;
    // foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
    // catalog = hvaultGetTableOptionString(foreigntableid, "catalog");
    // if (catalog)
    // {
    //     List *query = list_nth(fdw_plan_private, HvaultPlanCatalogQuery);
    //     HvaultCatalogCursor cursor = 
    //         hvaultCatalogInitCursor(query, CurrentMemoryContext);
    //     char const * query_str = hvaultCatalogGetQuery(cursor);
    //     ExplainPropertyText("Catalog query", query_str, es);
    //     hvaultCatalogFreeCursor(cursor);
    // }
    // else
    // {
    //     char *filename = hvaultGetTableOptionString(foreigntableid, "filename");
    //     if (filename != NULL)
    //         ExplainPropertyText("Single file", filename, es);
    // }

    // predicates = list_nth(fdw_plan_private, HvaultPlanPredicates);
    // pred_str = NIL;
    // foreach(l, predicates)
    // {
    //     GeomPredicate *pred = listToPredicate((List *) lfirst(l));
    //     StringInfoData str;
    //     char *colname;
    //     initStringInfo(&str);

    //     switch (pred->coltype) {
    //         case HvaultColumnFootprint:
    //             colname = "footprint";
    //             break;
    //         case HvaultColumnPoint:
    //             colname = "point";
    //             break;
    //         default:
    //             colname = "<unknown>";
    //     }

    //     if (pred->isneg)
    //         appendStringInfoString(&str, "NOT ");
    
    //     if (pred->op < HvaultGeomNumRealOpers) 
    //     {
    //         appendStringInfo(&str, "%s %s $%d", 
    //                          colname, hvaultGeomopstr[pred->op], pred->argno+1);
    //     }
    //     else
    //     {
    //         appendStringInfo(&str, "$%d %s %s", 
    //                          pred->argno+1, hvaultGeomopstr[pred->op], colname);
    //     }
    //     pred_str = lappend(pred_str, str.data);

    //     pfree(pred);
    // }
    // if (list_length(pred_str) > 0)
    //     ExplainPropertyList("Geometry predicates", pred_str, es);
}
