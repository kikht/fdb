#include "../driver.h"
#include "../interpolate.h"
#include "../options.h"

#define int8 hdf_int8
#include <hdf/mfhdf.h>
#undef int8

#define DEFAULT_SCANLINE_SIZE 10
#define FLAG_SHIFT_LONGITUDE 0x1
#define FLAG_HAS_FOOTPRINT   0x2
#define FLAG_HAS_POINT       0x4

const HvaultFileDriverMethods hvaultModisSwathMethods;

typedef struct 
{
    char const * cat_name;
    char const * filename;
    int32_t sd_id;

    UT_hash_handle hh;
} HvaultModisSwathFile;

typedef struct
{
    HvaultFileLayer layer;
    HvaultModisSwathFile * file;
    char const * sds_name;
    int32_t sds_id;
    int32_t sds_type;
    Oid coltypid;
} HvaultModisSwathLayer;

typedef struct 
{
    HvaultFileDriver driver;
    
    MemoryContext memctx;
    MemoryContext chunkmemctx;
    List * layers;
    HvaultModisSwathLayer *lat_layer, *lon_layer;
    HvaultModisSwathFile * files;
    float *lat_data, *lon_data, *lat_point_data, *lon_point_data;
    size_t num_lines, num_samples;
    size_t scanline_size;
    size_t cur_line;
    uint32_t flags;
} HvaultModisSwathDriver;

static HvaultModisSwathFile * 
getFile (HvaultModisSwathDriver * driver, char const * cat_name)
{
    HvaultModisSwathFile * file;
    if (cat_name == NULL)
        return NULL;

    HASH_FIND_STR(driver->files, cat_name, file);
    if (file == NULL)
    {
        file = palloc(sizeof(HvaultModisSwathFile));
        file->cat_name = cat_name;
        file->sd_id = FAIL;
        HASH_ADD_KEYPTR(hh, driver->files, file->cat_name, 
                        strlen(file->cat_name), file);
    }
    return file;
}

static HvaultDataType
mapHDFDatatype (int32_t type)
{
    switch (type)
    {
        case DFNT_CHAR8:   return HvaultInt8;
        case DFNT_UCHAR8:  return HvaultUInt8;
        case DFNT_INT8:    return HvaultInt8;
        case DFNT_UINT8:   return HvaultUInt8;
        case DFNT_INT16:   return HvaultInt16;
        case DFNT_UINT16:  return HvaultUInt16;
        case DFNT_INT32:   return HvaultInt32;
        case DFNT_UINT32:  return HvaultUInt32;
        case DFNT_FLOAT32: return HvaultFloat32;
        case DFNT_INT64:   return HvaultInt64;
        case DFNT_UINT64:  return HvaultUInt64;
        case DFNT_FLOAT64: return HvaultFloat64;
        default:
            elog(ERROR, "Unsupported HDF datatype %d", type);
            return HvaultInvalidDataType; /* Will never reach this */
    }
}

static HvaultFileDriver * 
hvaultModisSwathInit (List * table_options, MemoryContext memctx)
{
    HvaultModisSwathDriver * driver;
    MemoryContext oldmemctx, newmemctx;
    DefElem * def;

    newmemctx = AllocSetContextCreate(memctx, 
                                      "hvault modis swath driver", 
                                      ALLOCSET_DEFAULT_MINSIZE,
                                      ALLOCSET_DEFAULT_INITSIZE,
                                      ALLOCSET_DEFAULT_MAXSIZE);
    oldmemctx = MemoryContextSwitchTo(newmemctx);
    driver = palloc0(sizeof(HvaultModisSwathDriver));
    driver->memctx = newmemctx;
    driver->chunkmemctx = AllocSetContextCreate(newmemctx, 
                                                "hvault modis swath chunk", 
                                                ALLOCSET_SMALL_MINSIZE,
                                                ALLOCSET_SMALL_INITSIZE,
                                                ALLOCSET_SMALL_MAXSIZE);

    driver->flags = 0;
    def = defFindByName(table_options, "shift_longitude");
    if (def != NULL && defGetBoolean(def))
        driver->flags |= FLAG_SHIFT_LONGITUDE;

    def = defFindByName(table_options, HVAULT_COLUMN_OPTION_SCANLINE);
    driver->scanline_size = def != NULL ? defGetInt64(def) 
                                        : DEFAULT_SCANLINE_SIZE;

    driver->driver.methods = &hvaultModisSwathMethods;
    driver->driver.geotype = HvaultGeolocationCompact;

    MemoryContextSwitchTo(oldmemctx);
    return (HvaultFileDriver *) driver;
}

static void
hvaultModisSwathFree (HvaultFileDriver * drv)
{
    HvaultModisSwathDriver * driver = (HvaultModisSwathDriver *) drv;

    Assert(driver->driver.methods == &hvaultModisSwathMethods);
    MemoryContextDelete(driver->memctx);
}

static HvaultModisSwathLayer *
makeLayer()
{
    HvaultModisSwathLayer * res = palloc0(sizeof(HvaultModisSwathLayer));
    res->sds_id = -1;
    res->sds_type = -1;
    res->layer.colnum = -1;
    res->layer.type = HvaultInvalidLayerType;
    res->layer.src_type = HvaultInvalidDataType;
    res->layer.item_size = -1;
    res->layer.hfactor = 1;
    res->layer.vfactor = 1;
    return res;
}

static void 
addGeolocationColumns (HvaultModisSwathDriver * driver, List * options)
{
    HvaultModisSwathFile * file;
    DefElem * factor_option;
    int factor = 1;

    if (driver->lat_layer != NULL && driver->lon_layer != NULL)
        return;

    file = getFile(driver, 
                   defFindStringByName(options, HVAULT_COLUMN_OPTION_CATNAME));
    if (file == NULL)
    {
        elog(ERROR, "Catalog column is not specified for geolocation column");
        return; /* Will never reach here */
    }

    factor_option = defFindByName(options, HVAULT_COLUMN_OPTION_FACTOR);
    if (factor_option != NULL)
    {
        factor = defGetInt64(factor_option);
    }

    driver->lat_layer = makeLayer();
    driver->lat_layer->file = file;
    driver->lat_layer->sds_name = "Latitude";
    driver->lat_layer->layer.src_type = HvaultFloat32;
    driver->lat_layer->layer.item_size = sizeof(float);
    driver->lat_layer->layer.hfactor = factor;
    driver->lat_layer->layer.vfactor = factor;
    driver->layers = lappend(driver->layers, driver->lat_layer);

    driver->lon_layer = makeLayer();
    driver->lon_layer->file = file;
    driver->lon_layer->sds_name = "Longitude";
    driver->lon_layer->layer.src_type = HvaultFloat32;
    driver->lon_layer->layer.item_size = sizeof(float);
    driver->lon_layer->layer.hfactor = factor;
    driver->lon_layer->layer.vfactor = factor;
    driver->layers = lappend(driver->layers, driver->lon_layer);
}

static void 
addRegularColumn (HvaultModisSwathDriver * driver, 
                  Form_pg_attribute        attr, 
                  List                   * options)
{
    HvaultModisSwathFile * file;
    HvaultModisSwathLayer * layer;
    DefElem * def;

    layer = makeLayer();
    layer->sds_name = defFindStringByName(options, 
                                          HVAULT_COLUMN_OPTION_DATASET);
    if (layer->sds_name == NULL)
    {
        elog(ERROR, "Dataset column %s doesn't specify dataset name", 
             attr->attname.data);
        return; /* Will never reach here */
    }

    file = getFile(driver, defFindStringByName(options,
                                               HVAULT_COLUMN_OPTION_CATNAME));
    if (file == NULL)
    {
        elog(ERROR, "Catalog column is not specified for dataset column %s",
             attr->attname.data);
        return; /* Will never reach here */
    }
    layer->file = file;
    layer->layer.colnum = attr->attnum-1;
    layer->coltypid = attr->atttypid;
    switch (attr->atttypid)
    {
        /* Scaled value */
        case FLOAT8OID:
            layer->layer.temp = palloc(sizeof(double));
            break;
        /* Bitfield */
        case BITOID:
            layer->layer.src_type = HvaultBitmap;
            layer->layer.temp = palloc(VARBITTOTALLEN(attr->atttypmod));
            VARBITLEN(layer->layer.temp) = attr->atttypmod;
            SET_VARSIZE(layer->layer.temp, VARBITTOTALLEN(attr->atttypmod));
            layer->layer.item_size = VARBITBYTES(layer->layer.temp);
            break;
        /* Direct values */
        case FLOAT4OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
            /* nop */
            break;
            break;
            break;
            break;
            break;
        /* Unsupported */
        case VARBITOID:
            //TODO: maybe we can suport this
        default:
            elog(ERROR, "Column type for %s is not supported", 
                 attr->attname.data);
            break;
    }

    def = defFindByName(options, HVAULT_COLUMN_OPTION_FACTOR);
    if (def != NULL)
    {
        int64_t val = defGetInt64(def);
        layer->layer.hfactor = val;
        layer->layer.vfactor = val;
    }

    def = defFindByName(options, HVAULT_COLUMN_OPTION_HFACTOR);
    if (def != NULL)
        layer->layer.hfactor = defGetInt64(def);

    def = defFindByName(options, HVAULT_COLUMN_OPTION_VFACTOR);
    if (def != NULL)
        layer->layer.vfactor = defGetInt64(def);

    layer->layer.type = layer->layer.hfactor == 1 && layer->layer.vfactor == 1 ?
                        HvaultLayerSimple : HvaultLayerChunked;             

    Assert(layer != NULL);
    driver->layers = lappend(driver->layers, layer);
}

static void
checkGeometryColumn(Oid coltypid)
{
    Oid geomtypeoid = TypenameGetTypid("geometry");
    if (coltypid != geomtypeoid)
    {
        elog(ERROR, "Geolocation column must have geometry type");
    }
}

static void
hvaultModisSwathAddColumn (HvaultFileDriver * drv, 
                           Form_pg_attribute  attr, 
                           List             * options)
{
    HvaultModisSwathDriver * driver = (HvaultModisSwathDriver *) drv;
    MemoryContext oldmemctx;
    HvaultColumnType coltype;

    Assert(driver->driver.methods == &hvaultModisSwathMethods);
    oldmemctx = MemoryContextSwitchTo(driver->memctx);

    coltype = hvaultGetColumnType(defFindByName(options, 
                                      HVAULT_COLUMN_OPTION_TYPE));
    switch (coltype)
    {
        case HvaultColumnFootprint:
            checkGeometryColumn(attr->atttypid);
            driver->flags |= FLAG_HAS_FOOTPRINT;
            addGeolocationColumns(driver, options);
            break;
        case HvaultColumnPoint:
            checkGeometryColumn(attr->atttypid);
            driver->flags |= FLAG_HAS_POINT;
            addGeolocationColumns(driver, options);
            break;
        case HvaultColumnDataset:
            addRegularColumn(driver, attr, options);
            break;
        default:
            elog(ERROR, "Column type is not supported by driver");
    }    

    MemoryContextSwitchTo(oldmemctx);
}

static void 
hvaultModisSwathClose (HvaultFileDriver * drv)
{
    HvaultModisSwathDriver * driver = (HvaultModisSwathDriver *) drv;
    ListCell *l;
    HvaultModisSwathFile * file;

    Assert(driver->driver.methods == &hvaultModisSwathMethods);

    foreach(l, driver->layers)
    {
        HvaultModisSwathLayer * layer = lfirst(l);
        if (layer->sds_id == FAIL) 
            continue;

        if (SDendaccess(layer->sds_id) == FAIL)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), 
                            errmsg("Can't close SDS")));
        }

        layer->sds_id = FAIL;
    }

    for (file = driver->files; file != NULL; file = file->hh.next)
    {
        if (file->sd_id == FAIL) 
            continue;
        if (SDend(file->sd_id) == FAIL)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Can't close HDF file")));
        }
        
        file->sd_id = FAIL;
        file->filename = NULL;
    }
}

static void 
hvaultModisSwathOpen (HvaultFileDriver        * drv,
                      HvaultCatalogItem const * products)
{
    HvaultModisSwathDriver * driver = (HvaultModisSwathDriver *) drv;
    MemoryContext oldmemctx;
    ListCell *l;

    Assert(driver->driver.methods == &hvaultModisSwathMethods);

    hvaultModisSwathClose(drv);
    driver->num_samples = 0;
    driver->num_lines = 0;
    driver->cur_line = 0;

    if (list_length(driver->layers) == 0)
    {
        elog(ERROR, "Query must contain at least one dataset or geolocation column");
        return;
    }

    oldmemctx = MemoryContextSwitchTo(driver->memctx);
    {
        HvaultModisSwathFile *file;
        for (file = driver->files; file != NULL; file = file->hh.next)
        {
            HvaultCatalogItem const * filename;

            HASH_FIND_STR(products, file->cat_name, filename);
            if (filename == NULL)
            {
                elog(ERROR, "Can't find catalog column value for %s", 
                     file->cat_name);
                return; /*Will never reach this */
            }

            if (filename->str == NULL)
                continue;

            file->filename = filename->str;
            elog(DEBUG1, "loading hdf file %s", file->filename);
            file->sd_id = SDstart(file->filename, DFACC_READ);
            if (file->sd_id == FAIL)
            {
                elog(WARNING, "Can't open HDF file %s, skipping file", 
                     file->filename);
            }
        }
    }

    //TODO: add support for sparse datasets
    foreach(l, driver->layers)
    {
        HvaultModisSwathLayer *layer = lfirst(l);
        int32_t sds_idx, rank, dims[H4_MAX_VAR_DIMS], sdnattrs, sdtype;
        size_t norm_lines, norm_samples, layer_lines, layer_samples;
        HvaultDataType cur_dataype;
        double cal_err, offset_err;

        elog(DEBUG2, "Opening SDS %s", layer->sds_name);

        if (layer->file->sd_id == FAIL)
        {
            elog(DEBUG2, "File is not opened, skipping dataset %s", 
                 layer->sds_name);
            continue;
        }

        /* Find SDS */
        sds_idx = SDnametoindex(layer->file->sd_id, layer->sds_name);
        if (sds_idx == FAIL)
        {
            elog(WARNING, "Can't find dataset %s in file %s, skipping",
                 layer->sds_name, layer->file->filename);
            continue;
        }
        /* Select SDS */
        layer->sds_id = SDselect(layer->file->sd_id, sds_idx);
        if (layer->sds_id == FAIL)
        {
            elog(WARNING, "Can't open dataset %s in file %s, skipping",
                 layer->sds_name, layer->file->filename);
            continue;
        }
        /* Get dimension sizes */
        if (SDgetinfo(layer->sds_id, NULL, &rank, dims, &layer->sds_type, 
                      &sdnattrs) == FAIL)
        {
            elog(WARNING, "Can't get info about %s in file %s, skipping",
                 layer->sds_name, layer->file->filename);
            SDendaccess(layer->sds_id);
            layer->sds_id = FAIL;
            continue;
        }
        
        /* Check dimensions correctness */
        //TODO: add bitmap dimension support
        //TODO: add dimension prefix/postfix support
        if (rank != 2)
        {
            elog(WARNING, "SDS %s in file %s has %dd dataset, skipping",
                 layer->sds_name, layer->file->filename, rank);
            SDendaccess(layer->sds_id);
            layer->sds_id = FAIL;
            continue;
        }
        layer_lines = dims[0];
        layer_samples = dims[1];
        norm_lines = layer_lines * layer->layer.vfactor;
        norm_samples = layer_samples * layer->layer.hfactor;

        if (norm_samples < driver->scanline_size)
        {
            elog(WARNING, "SDS %s in file %s has only %d lines, skipping",
                 layer->sds_name, layer->file->filename, dims[0]);
            SDendaccess(layer->sds_id);
            layer->sds_id = FAIL;
            continue;
        }
        if (driver->num_lines == 0)
        {
            driver->num_lines = norm_lines;
        } 
        else if (driver->num_lines != norm_lines)
        {
            elog(WARNING, "SDS %s in file %s with %lu lines is incompatible with others (%lu), skipping",
                 layer->sds_name, layer->file->filename, 
                 norm_lines, driver->num_lines);
            SDendaccess(layer->sds_id);
            layer->sds_id = FAIL;
            continue;
        }
        if (driver->num_samples == 0)
        {
            driver->num_samples = norm_samples;
        } 
        else if (norm_samples != driver->num_samples)
        {
            elog(WARNING, "SDS %s in file %s with %lu samples is incompatible with others (%lu), skipping",
                 layer->sds_name, layer->file->filename, 
                 norm_samples, driver->num_samples);
            SDendaccess(layer->sds_id);
            layer->sds_id = FAIL;
            continue;
        }

        /* Check SDS datatype */
        cur_dataype = mapHDFDatatype(layer->sds_type);
        /* Initialize src_type if unknown yet */
        if (layer->layer.src_type == HvaultInvalidDataType)
        {
            /* Fill src_type, item_size and check coltype compatibility */
            bool res;
            switch (layer->coltypid)
            {
                case FLOAT8OID:
                    res = true;
                    break;
                case FLOAT4OID:
                    res = cur_dataype == HvaultFloat32;
                    break;
                case INT2OID:
                    res = cur_dataype >= HvaultInt8 && 
                          cur_dataype <= HvaultUInt16;
                    break;
                case INT4OID:
                    res = cur_dataype >= HvaultInt8 && 
                          cur_dataype <= HvaultUInt32;
                    break;
                case INT8OID:
                    res = cur_dataype >= HvaultInt8 && 
                          cur_dataype <= HvaultUInt64;
                    break;
                default:
                    elog(ERROR, "Unsupported column type");
                    return; /* Will never reach this */
            }
            if (!res)
            {
                elog(WARNING, "SDS %s in file %s has datatype incompatible with column type",
                     layer->sds_name, layer->file->filename);
                SDendaccess(layer->sds_id);
                layer->sds_id = FAIL;
                continue;
            }
            layer->layer.src_type = cur_dataype;
            layer->layer.item_size = hvaultDatatypeSize[cur_dataype];
        } 
        /* Handle bitmaps specially */
        else if (layer->layer.src_type == HvaultBitmap)
        {
            //TODO: add bitmap dimension support
            size_t bit_layers_size = 1;
            if (layer->layer.item_size != 
                bit_layers_size * hvaultDatatypeSize[cur_dataype])
            {
                elog(WARNING, "SDS %s in file %s has incompatible bitfield size",
                     layer->sds_name, layer->file->filename);
                SDendaccess(layer->sds_id);
                layer->sds_id = FAIL;
                continue;
            }

        }
        /* Check that type is equal to previous files */
        else if (layer->layer.src_type != cur_dataype)
        {
            elog(WARNING, "SDS %s in file %s has datatype %d incompatible with previous files (%d)",
                 layer->sds_name, layer->file->filename, 
                 cur_dataype, layer->layer.src_type);
            SDendaccess(layer->sds_id);
            layer->sds_id = FAIL;
            continue;
        }

        /* Get scale, offset & fill */
        if (layer->layer.src_type != HvaultBitmap)
        {
            if (layer->layer.fill_val == NULL)
                layer->layer.fill_val = palloc(layer->layer.item_size);
            if (SDgetfillvalue(layer->sds_id, layer->layer.fill_val) != SUCCEED)
            {
                pfree(layer->layer.fill_val);
                layer->layer.fill_val = NULL;
            }
        }
        if (layer->coltypid == FLOAT8OID)
        {
            if (SDgetcal(layer->sds_id, &layer->layer.scale, &cal_err, 
                         &layer->layer.offset, &offset_err, &sdtype) != SUCCEED)
            {
                layer->layer.scale = 1.;
                layer->layer.offset = 0;
            }
        } 
        /* Allocate data buffer */
        if (layer->layer.data == NULL)
            layer->layer.data = palloc(layer->layer.item_size * layer_samples * 
                (driver->scanline_size / layer->layer.vfactor));
    }
    /* Sanity check */
    if (driver->num_lines == 0 || driver->num_samples == 0)
    {
        elog(WARNING, "Can't get number of lines and samples, skipping record");
        MemoryContextSwitchTo(oldmemctx);
        hvaultModisSwathClose(drv);
        return;
    }
    /* Check that geolocation is available */
    if ((driver->flags & (FLAG_HAS_FOOTPRINT | FLAG_HAS_POINT)) &&
            (  driver->lat_layer->sds_id == FAIL 
            || driver->lon_layer->sds_id == FAIL))
    {
        elog(WARNING, "Can't open geolocation layers, skipping file");
        MemoryContextSwitchTo(oldmemctx);
        hvaultModisSwathClose(drv);
        return;
    }
    /* Allocate point buffers if necessary */
    if (driver->lat_point_data == NULL &&
        driver->flags & FLAG_HAS_POINT && 
        driver->lat_layer->layer.hfactor != 1)
    {
        driver->lat_point_data = palloc(sizeof(float) * driver->num_samples * 
                                        driver->scanline_size);
        driver->lon_point_data = palloc(sizeof(float) * driver->num_samples * 
                                        driver->scanline_size);
    }
    /* Allocate footprint buffers */
    if (driver->lat_data == NULL &&
        driver->flags & FLAG_HAS_FOOTPRINT)
    {
        driver->lat_data = palloc(sizeof(float) * (driver->num_samples + 1) *
                                  (driver->scanline_size + 1));
        driver->lon_data = palloc(sizeof(float) * (driver->num_samples + 1) *
                                  (driver->scanline_size + 1));
    }

    MemoryContextSwitchTo(oldmemctx);
}

static void 
hvaultModisSwathRead (HvaultFileDriver * drv,
                      HvaultFileChunk  * chunk)
{
    HvaultModisSwathDriver * driver = (HvaultModisSwathDriver *) drv;
    MemoryContext oldmemctx;
    ListCell *l;
    size_t geo_lines, geo_samples;
    int geo_factor;

    Assert(driver->driver.methods == &hvaultModisSwathMethods);
    MemoryContextReset(driver->chunkmemctx);
    oldmemctx = MemoryContextSwitchTo(driver->chunkmemctx);

    if (driver->cur_line >= driver->num_lines)
    {
        chunk->size = 0;
        return;
    }

    chunk->const_layers = NIL;
    chunk->layers = NIL;
    chunk->lat = driver->lat_data;
    chunk->lon = driver->lon_data;
    chunk->point_lat = driver->lat_point_data;
    chunk->point_lon = driver->lon_point_data;
    chunk->stride = driver->num_samples;
    chunk->size = driver->num_samples * driver->scanline_size;

    /* Read data from file */
    foreach(l, driver->layers)
    {
        HvaultModisSwathLayer *layer = lfirst(l);
        int32_t start[H4_MAX_VAR_DIMS], stride[H4_MAX_VAR_DIMS], 
                end[H4_MAX_VAR_DIMS];

        if (layer->sds_id == FAIL)
            continue;

        //TODO: add bitmap dimension support
        //TODO: add dimension prefix support
        start[0] = driver->cur_line / layer->layer.vfactor;
        start[1] = 0;
        stride[0] = 1;
        stride[1] = 1;
        end[0] = driver->scanline_size / layer->layer.vfactor;
        end[1] = driver->num_samples / layer->layer.hfactor;

        if (SDreaddata(layer->sds_id, start, stride, end, 
                       layer->layer.data) == FAIL)
        {
            elog(ERROR, "Can't read data from %s dataset %s", 
                 layer->file->filename, layer->sds_name);
            return; /* will never reach here */
        }

        if (layer->layer.colnum >= 0)
            chunk->layers = lappend(chunk->layers, layer);
    }

    if (driver->flags & (FLAG_HAS_FOOTPRINT | FLAG_HAS_POINT))
    {
        geo_factor = driver->lat_layer->layer.vfactor;
        Assert(driver->lat_layer);
        Assert(driver->lon_layer);
        Assert(driver->lat_layer->layer.hfactor == geo_factor);
        Assert(driver->lat_layer->layer.vfactor == geo_factor);
        Assert(driver->lon_layer->layer.hfactor == geo_factor);
        Assert(driver->lon_layer->layer.vfactor == geo_factor);
        geo_lines = driver->scanline_size / geo_factor;
        geo_samples = driver->num_samples / geo_factor;

        /* Shift longitude */
        if (driver->flags & FLAG_SHIFT_LONGITUDE)
        {
            size_t i;
            const size_t size = geo_lines * geo_samples;
            float * const buf = driver->lon_layer->layer.data;
            for (i = 0; i < size; i++)
            {
                buf[i] += (float)(360 * (buf[i] < 0));
            }
        }

        /* Calc point geolocation */
        if (driver->flags & FLAG_HAS_POINT)
        {
            switch (geo_factor)
            {
                case 1:
                    chunk->point_lat = driver->lat_layer->layer.data;
                    chunk->point_lon = driver->lon_layer->layer.data;
                    break;
                case 2:
                    hvaultInterpolatePoints2x(driver->lat_layer->layer.data, 
                                              driver->lat_point_data, 
                                              geo_lines, geo_samples);
                    hvaultInterpolatePoints2x(driver->lon_layer->layer.data, 
                                              driver->lon_point_data, 
                                              geo_lines, geo_samples);
                    break;
                case 4:
                    hvaultInterpolatePoints4x(driver->lat_layer->layer.data, 
                                              driver->lat_point_data, 
                                              geo_lines, geo_samples);
                    hvaultInterpolatePoints4x(driver->lon_layer->layer.data, 
                                              driver->lon_point_data, 
                                              geo_lines, geo_samples);
                default:
                {
                    /* TODO: move kernel generation to initialization stage */
                    float * kernel = palloc(sizeof(float) * 16 * 
                                            geo_factor * geo_factor);
                    hvaultInterpolatePointKernel(geo_factor, kernel);
                    hvaultInterpolatePoints(driver->lat_layer->layer.data, 
                                            driver->lat_point_data, 
                                            geo_lines, geo_samples, 
                                            kernel, geo_factor);
                    hvaultInterpolatePoints(driver->lon_layer->layer.data, 
                                            driver->lon_point_data, 
                                            geo_lines, geo_samples, 
                                            kernel, geo_factor);
                    pfree(kernel);
                }
                    break;
            }
        }

        /* Calc footprint geolocation */
        if (driver->flags & FLAG_HAS_FOOTPRINT)
        {
            switch (geo_factor)
            {
                case 1:
                    hvaultInterpolateFootprint1x(driver->lat_layer->layer.data, 
                                                 driver->lat_data, 
                                                 geo_lines, geo_samples);
                    hvaultInterpolateFootprint1x(driver->lon_layer->layer.data, 
                                                 driver->lon_data, 
                                                 geo_lines, geo_samples);
                    break;
                case 2:
                    hvaultInterpolateFootprint2x(driver->lat_layer->layer.data, 
                                                 driver->lat_data, 
                                                 geo_lines, geo_samples);
                    hvaultInterpolateFootprint2x(driver->lon_layer->layer.data, 
                                                 driver->lon_data, 
                                                 geo_lines, geo_samples);
                    break;
                case 4:
                    hvaultInterpolateFootprint4x(driver->lat_layer->layer.data, 
                                                 driver->lat_data, 
                                                 geo_lines, geo_samples);
                    hvaultInterpolateFootprint4x(driver->lon_layer->layer.data, 
                                                 driver->lon_data, 
                                                 geo_lines, geo_samples);
                default:
                {
                    /* TODO: move kernel generation to initialization stage */
                    float * kernel = palloc(sizeof(float) * 4 * 
                                            (2 * geo_factor + 1) * 
                                            (2 * geo_factor + 1));
                    hvaultInterpolatePointKernel(geo_factor, kernel);
                    hvaultInterpolateFootprint(driver->lat_layer->layer.data, 
                                               driver->lat_data, 
                                               geo_lines, geo_samples, 
                                               kernel, geo_factor);
                    hvaultInterpolateFootprint(driver->lon_layer->layer.data, 
                                               driver->lon_data, 
                                               geo_lines, geo_samples, 
                                               kernel, geo_factor);
                    pfree(kernel);
                }
                    break;
            }
        }
    }

    driver->cur_line += driver->scanline_size;    
    MemoryContextSwitchTo(oldmemctx);
}

const HvaultFileDriverMethods hvaultModisSwathMethods = 
{
    hvaultModisSwathInit,
    hvaultModisSwathAddColumn,
    hvaultModisSwathOpen,
    hvaultModisSwathRead,
    hvaultModisSwathClose,
    hvaultModisSwathFree
};
