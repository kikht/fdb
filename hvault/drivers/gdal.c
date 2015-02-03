#include <gdal/gdal.h>
#include <gdal/ogr_srs_api.h>

#include "../driver.h"
#include "../options.h"

#define FLAG_SHIFT_LONGITUDE 0x1
#define FLAG_HAS_FOOTPRINT   0x2
#define FLAG_HAS_POINT       0x4
#define FLAG_INVERSE_SCALE   0x8

#define DEFAULT_GEOCACHE_SIZE 10

const HvaultFileDriverMethods hvaultGDALMethods;
bool hvaultGDALDestructorRegistered = false;

typedef struct 
{
    HvaultFileLayer layer;

    char const * template;
    char const * cat_name;
    char const * attname;
    
    GDALDatasetH dataset;
    GDALRasterBandH band;
    size_t num_lines, num_samples;
    GDALDataType gdal_type;
    Oid coltypid;
    char const * dataset_name;
    uint32_t flags;
} HvaultGDALLayer;

typedef struct HvaultGDALGeolocation
{
    char const * tile;
    float *fp_lat, *fp_lon, *point_lat, *point_lon;
    struct HvaultGDALGeolocation * next;
} HvaultGDALGeolocation;

typedef struct
{
    HvaultFileDriver driver;

    MemoryContext memctx;
    MemoryContext chunkmemctx;
    List * layers;
    char const * tile_col;
    char const * tile;
    OGRSpatialReferenceH dstspref;
    size_t num_lines, num_samples;
    
    double aft[6];
    OGRSpatialReferenceH srcspref;
    OGRCoordinateTransformationH transform;
    HvaultGDALGeolocation *geo_cache, *geo_cache_last;
    size_t geo_cache_size;
    double *lat_temp, *lon_temp;
    
    bool read_complete; /* TODO: use chunked read */
    uint32_t flags;
} HvaultGDALDriver;


/* 
 * We should be especially careful with throwing errors in cleanup funcs 
 * to avoid memory leaks in GDAL
 */
static void
hvaultGDALCloseLayer (HvaultGDALLayer * layer)
{
    if (layer->dataset != NULL)
    {
        GDALClose(layer->dataset);
        layer->dataset = NULL;
    }

    layer->band = NULL;
}


static void 
hvaultGDALClose (HvaultFileDriver * drv)
{
    HvaultGDALDriver * driver = (HvaultGDALDriver *) drv;
    ListCell *l;

    Assert(driver->driver.methods == &hvaultGDALMethods);
    
    if (driver->transform != NULL)
    {
        OCTDestroyCoordinateTransformation(driver->transform);
        driver->transform = NULL;
    }

    if (driver->srcspref != NULL)
    {
        OSRDestroySpatialReference(driver->srcspref);
        driver->srcspref = NULL;
    }

    foreach(l, driver->layers)
    {
        HvaultGDALLayer * layer = lfirst(l);
        hvaultGDALCloseLayer(layer);        
    }

    driver->read_complete = true;
}

static void
hvaultGDALFree (HvaultFileDriver * drv)
{
    HvaultGDALDriver * driver = (HvaultGDALDriver *) drv;

    Assert(driver->driver.methods == &hvaultGDALMethods);
    hvaultGDALClose(drv);

    if (driver->dstspref != NULL)
    {
        OSRDestroySpatialReference(driver->dstspref);
        driver->dstspref = NULL;
    }

    MemoryContextDelete(driver->memctx);
}

static HvaultFileDriver *
hvaultGDALInit (List * table_options, MemoryContext memctx)
{
    HvaultGDALDriver * driver;
    MemoryContext oldmemctx, newmemctx;
    DefElem * def;

    newmemctx = AllocSetContextCreate(memctx, 
                                      "hvault_gdal_driver",
                                      ALLOCSET_DEFAULT_MINSIZE,
                                      ALLOCSET_DEFAULT_INITSIZE,
                                      ALLOCSET_DEFAULT_MAXSIZE);
    oldmemctx = MemoryContextSwitchTo(newmemctx);
    driver = palloc0(sizeof(HvaultGDALDriver));
    driver->memctx = newmemctx;
    driver->chunkmemctx = AllocSetContextCreate(newmemctx, 
                                                "hvault modis swath chunk", 
                                                ALLOCSET_SMALL_MINSIZE,
                                                ALLOCSET_SMALL_INITSIZE,
                                                ALLOCSET_SMALL_MAXSIZE);
    
    driver->driver.methods = &hvaultGDALMethods;
    driver->driver.geotype = HvaultGeolocationCompact;
    driver->geo_cache_size = DEFAULT_GEOCACHE_SIZE;
    
    driver->flags = 0;
    def = defFindByName(table_options, HVAULT_TABLE_OPTION_SHIFT_LONGITUDE);
    if (def != NULL && defGetBoolean(def))
        driver->flags |= FLAG_SHIFT_LONGITUDE;
    
    driver->dstspref = OSRNewSpatialReference(SRS_WKT_WGS84);
    if (driver->dstspref == NULL) {
        elog(ERROR, "Can't create WGS84 spatial reference");
        return NULL; /* Will never reach here */
    }

    /* Indicate that no file is currently opened */
    driver->read_complete = true;

    MemoryContextSwitchTo(oldmemctx);
    return (HvaultFileDriver *) driver;
}



static HvaultGDALLayer * 
makeLayer()
{
    HvaultGDALLayer * res = palloc0(sizeof(HvaultGDALLayer));
    
    /* TODO: move this part to common func */
    res->layer.colnum = -1;
    res->layer.type = HvaultInvalidLayerType;
    res->layer.src_type = HvaultInvalidDataType;
    res->layer.item_size = -1;
    res->layer.hfactor = 1;
    res->layer.vfactor = 1;
    return res;
}

static void 
addRegularColumn (HvaultGDALDriver  * driver, 
                  Form_pg_attribute   attr, 
                  List              * options)
{
    HvaultGDALLayer * layer;
    DefElem * def;
 
    layer = makeLayer();
    Assert(layer != NULL);

    layer->attname = attr->attname.data;
    layer->template = defFindStringByName(options,
                                          HVAULT_COLUMN_OPTION_DATASET);
    if (layer->template == NULL)
    {
        elog(ERROR, "Dataset column %s doesn't specify dataset name", 
             attr->attname.data);
        return; /* Will never reach here */
    }
    layer->cat_name = defFindStringByName(options,
                                          HVAULT_COLUMN_OPTION_CATNAME);
    if (layer->cat_name == NULL)
    {
        elog(ERROR, "Catalog column is not specified for dataset column %s",
             attr->attname.data);
        return; /* Will never reach here */
    }

    /* TODO: move to common function */ 
    layer->layer.colnum = attr->attnum - 1;
    def = defFindByName(options, HVAULT_COLUMN_OPTION_FACTOR);
    if (def != NULL)
    {
        int64_t val = defGetInt(def);
        layer->layer.hfactor = val;
        layer->layer.vfactor = val;
    }

    def = defFindByName(options, HVAULT_COLUMN_OPTION_HFACTOR);
    if (def != NULL)
        layer->layer.hfactor = defGetInt(def);

    def = defFindByName(options, HVAULT_COLUMN_OPTION_VFACTOR);
    if (def != NULL)
        layer->layer.vfactor = defGetInt(def);

    layer->layer.type = layer->layer.hfactor == 1 && layer->layer.vfactor == 1 ?
                        HvaultLayerSimple : HvaultLayerChunked;

    /* TODO: Add support for other types */
    layer->coltypid = attr->atttypid;
    switch (attr->atttypid)
    {
        /* Scaled value */
        case FLOAT8OID:
            layer->layer.temp = palloc(sizeof(double));
            break;
        /* Direct values */
        case FLOAT4OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
            /* nop */
            break;
        /* Unsupported */
        case BITOID:
        case VARBITOID:
            //TODO: maybe we can suport this
        default:
            elog(ERROR, "Column type for %s is not supported", 
                 attr->attname.data);
            break;
    }
    
    def = defFindByName(options, HVAULT_COLUMN_OPTION_INVERSE_SCALE);
    if (def != NULL && defGetBoolean(def))
    {
        layer->flags |= FLAG_INVERSE_SCALE;
    }

    driver->layers = lappend(driver->layers, layer);
}

static void
hvaultGDALAddColumn (HvaultFileDriver        * drv,
                     Form_pg_attribute         attr,
                     List                    * options)
{
    PG_TRY();
    {
        HvaultGDALDriver * driver = (HvaultGDALDriver *) drv;
        MemoryContext oldmemctx;
        HvaultColumnType coltype;

        Assert(driver->driver.methods == &hvaultGDALMethods);
        oldmemctx = MemoryContextSwitchTo(driver->memctx);

        coltype = hvaultGetColumnType(defFindByName(options, 
                                          HVAULT_COLUMN_OPTION_TYPE));
        switch (coltype)
        {
            case HvaultColumnFootprint:
                driver->flags |= FLAG_HAS_FOOTPRINT;
                driver->tile_col = defFindStringByName(options,
                                               HVAULT_COLUMN_OPTION_CATNAME);

                /* TODO: add only-geolocation support */
                break;
            case HvaultColumnPoint:
                driver->flags |= FLAG_HAS_POINT;
                driver->tile_col = defFindStringByName(options,
                                               HVAULT_COLUMN_OPTION_CATNAME);
                /* TODO: add only-geolocation support */
                break;
            case HvaultColumnDataset:
                addRegularColumn(driver, attr, options);
                break;
            default:
                elog(ERROR, "Column type is not supported by driver");
        }    

        MemoryContextSwitchTo(oldmemctx);
    }
    PG_CATCH();
    {
        hvaultGDALFree(drv);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static char * 
makeDatasetName (char const * filename, char const * template)
{
    StringInfoData res;
    char const *cur = template;

    initStringInfo(&res);
    while (*cur) 
    {
        if (*cur == '%') 
        {
            cur++;
            switch (*cur)
            {
                case 'f':
                    appendStringInfoString(&res, filename);
                    break;
                case '%':
                    appendStringInfoCharMacro(&res, '%');
                    break;
                default:
                    elog(ERROR, "Unknown control symbol %c in format string %s",
                         *cur, template);
            }
        }
        else
        {
            appendStringInfoCharMacro(&res, *cur);
        }
        cur++;
    }
    return res.data; 
}

static HvaultDataType
mapGDALDatatype(GDALDataType gdal_type)
{
    switch (gdal_type)
    {
        case GDT_Byte:    return HvaultUInt8;
        case GDT_UInt16:  return HvaultUInt16;
        case GDT_Int16:   return HvaultInt16;
        case GDT_UInt32:  return HvaultUInt32;
        case GDT_Int32:   return HvaultInt32;
        case GDT_Float32: return HvaultFloat32; 
        case GDT_Float64: return HvaultFloat64;

        case GDT_CInt16:
        case GDT_CInt32:
        case GDT_CFloat32:
        case GDT_CFloat64:
        case GDT_Unknown:
        default:
            elog(ERROR, "Unsupported GDAL datatype %d", gdal_type);
            return HvaultInvalidDataType; /* Will never reach this */
    }
}

static bool
checkColumnType(Oid coltypid, HvaultDataType cur_datatype) 
{
     switch (coltypid)
     {
         case FLOAT8OID:
             return true;
         case FLOAT4OID:
             return cur_datatype == HvaultFloat32;
         case INT2OID:
             return cur_datatype >= HvaultInt8 && 
                    cur_datatype <= HvaultUInt16;
         case INT4OID:
             return cur_datatype >= HvaultInt8 && 
                    cur_datatype <= HvaultUInt32;
         case INT8OID:
             return cur_datatype >= HvaultInt8 && 
                    cur_datatype <= HvaultUInt64;
         default:
             elog(ERROR, "Unsupported column type");
             return false; /* Will never reach this */
     }
}

static void
doubleToType(double val, HvaultDataType type, void * buf)
{
    switch (type)
    {
        case HvaultInt8:    *((  int8_t *) buf) = val; break;
        case HvaultUInt8:   *(( uint8_t *) buf) = val; break;
        case HvaultInt16:   *(( int16_t *) buf) = val; break;
        case HvaultUInt16:  *((uint16_t *) buf) = val; break;
        case HvaultInt32:   *(( int32_t *) buf) = val; break;
        case HvaultUInt32:  *((uint32_t *) buf) = val; break;
        case HvaultInt64:   *(( int64_t *) buf) = val; break;
        case HvaultUInt64:  *((uint64_t *) buf) = val; break;

        case HvaultFloat32: *((   float *) buf) = val; break;
        case HvaultFloat64: *((  double *) buf) = val; break;

        case HvaultBitmap:
        case HvaultPrefixBitmap:
        default:
            elog(ERROR, "Unsupported column type");
            return; /* Will never reach this */
    }
}

static HvaultGDALGeolocation *
createGeolocation (HvaultGDALDriver * driver)
{
    HvaultGDALGeolocation * res = palloc0(sizeof(HvaultGDALGeolocation));

    if (driver->flags & FLAG_HAS_FOOTPRINT)
    {
        res->fp_lat = palloc(sizeof(float) 
                             * (driver->num_samples + 1) 
                             * (driver->num_lines + 1));
        res->fp_lon = palloc(sizeof(float) 
                             * (driver->num_samples + 1) 
                             * (driver->num_lines + 1));
    }

    if (driver->flags & FLAG_HAS_POINT)
    {
        res->point_lat = palloc(sizeof(float) 
                                * driver->num_samples 
                                * driver->num_lines);
        res->point_lon = palloc(sizeof(float) 
                                * driver->num_samples 
                                * driver->num_lines);
    }

    return res;
}

static void 
hvaultGDALOpen (HvaultFileDriver        * drv, 
                HvaultCatalogItem const * products)
{
    MemoryContext oldmemctx = NULL;
    PG_TRY();
    {
        HvaultGDALDriver * driver = (HvaultGDALDriver *) drv;
        ListCell *l;

        Assert(driver->driver.methods == &hvaultGDALMethods);
        hvaultGDALClose(drv);

        if (list_length(driver->layers) == 0)
        {
            elog(ERROR, 
            "Query must contain at least one dataset or geolocation column");
            return;
        }

        oldmemctx = MemoryContextSwitchTo(driver->memctx);
        
        foreach(l, driver->layers)
        {
            HvaultGDALLayer *layer = lfirst(l);
            HvaultCatalogItem const * filename;
            size_t num_rasters;
            size_t norm_lines, norm_samples;
            HvaultDataType cur_datatype;

            HASH_FIND_STR(products, layer->cat_name, filename);
            if (filename == NULL)
            {
                elog(ERROR, "Can't find catalog column value for %s", 
                     layer->cat_name);
                return; /*Will never reach this */
            }

            if (filename->str == NULL)
            {
                elog(DEBUG2, "File is not opened, skipping dataset %s", 
                     layer->template);
                continue;
            }

            layer->dataset_name = makeDatasetName(filename->str, 
                                                  layer->template);
            layer->dataset = GDALOpen(layer->dataset_name, GA_ReadOnly);
            if (layer->dataset == NULL)
            {
                elog(WARNING, "Can't open dataset %s, skipping", 
                     layer->dataset_name);
                continue;
            }

            if (!hvaultGDALDestructorRegistered) {
                char const * driver_name = 
                    GDALGetDriverShortName(
                        GDALGetDatasetDriver(layer->dataset));
                if (strcmp(driver_name, "HDF4") == 0) {
                    atexit(GDALDestroyDriverManager);
                    hvaultGDALDestructorRegistered = true;
                }
            }

            layer->num_lines = GDALGetRasterXSize(layer->dataset);
            layer->num_samples = GDALGetRasterYSize(layer->dataset);
            num_rasters = GDALGetRasterCount(layer->dataset);

            norm_lines = layer->num_lines * layer->layer.vfactor;
            norm_samples = layer->num_samples * layer->layer.hfactor;
           
            /* TODO: support datasets of different size */
            if (driver->num_lines == 0) 
            {
                driver->num_lines = norm_lines;
            }
            else if (driver->num_lines != norm_lines)
            {
                elog(WARNING, 
                     "Dataset %s size is incompatible with others, skipping",
                     layer->dataset_name);
                MemoryContextSwitchTo(oldmemctx);
                hvaultGDALCloseLayer(layer);
                continue;
            }

            if (driver->num_samples == 0)
            {
                driver->num_samples = norm_samples;
            }
            else if (driver->num_samples != norm_samples)
            {
                elog(WARNING, 
                     "Dataset %s size is incompatible with others, skipping",
                     layer->dataset_name);
                MemoryContextSwitchTo(oldmemctx);
                hvaultGDALCloseLayer(layer);
                continue;
            }
            
            /* TODO: Add support for multiband images */
            if (num_rasters != 1)
            {
                elog(WARNING, "Dataset %s contains %d bands", 
                     layer->dataset_name, (int) num_rasters);
            }
           
            /* Initialize geolocation transform */
            /* TODO: check that geolocation is the same for all files */
            if (driver->transform == NULL && 
                    (driver->flags & (FLAG_HAS_POINT | FLAG_HAS_FOOTPRINT)))
            {
                if (GDALGetGeoTransform(layer->dataset, driver->aft) == CE_None)
                {
                    driver->srcspref = OSRNewSpatialReference(
                            GDALGetProjectionRef(layer->dataset));
                    if (driver->srcspref != NULL)
                    {
                        driver->transform = 
                            OCTNewCoordinateTransformation(driver->srcspref, 
                                                           driver->dstspref);
                        if (driver->transform != NULL)
                        {
                            driver->aft[1] /= (double) layer->layer.hfactor;
                            driver->aft[4] /= (double) layer->layer.hfactor;
                            driver->aft[2] /= (double) layer->layer.vfactor;
                            driver->aft[5] /= (double) layer->layer.vfactor;
                        }
                        else
                        {
                            elog(WARNING, 
                                 "Can't create projection transform for %s",
                                 layer->dataset_name);
                            OSRDestroySpatialReference(driver->srcspref);
                            driver->srcspref = NULL;
                        }
                    }
                    else
                    {
                        elog(WARNING, 
                             "Can't get dataset %s projection reference",
                             layer->dataset_name);
                    }
                }
                else 
                {
                    elog(WARNING,
                         "Can't get dataset %s affine transformation",
                         layer->dataset_name);
                }
            }


            layer->band = GDALGetRasterBand(layer->dataset, 1);
            if (layer->band == NULL)
            {
                elog(WARNING,
                     "Can't get raster band for %s, skipping",
                     layer->dataset_name);
                MemoryContextSwitchTo(oldmemctx);
                hvaultGDALCloseLayer(layer);
                continue;
            }

            /* TODO: add bitmap support */
            layer->gdal_type = GDALGetRasterDataType(layer->band);
            cur_datatype = mapGDALDatatype(layer->gdal_type);
            /* Initialize src_type if unknown yet */
            if (layer->layer.src_type == HvaultInvalidDataType)
            {
                if (!checkColumnType(layer->coltypid, cur_datatype))
                {
                    elog(WARNING, 
                         "Dataset %s has incompatible datatype %d, skipping",
                         layer->dataset_name, cur_datatype);
                    hvaultGDALCloseLayer(layer);
                    continue;
                }
                layer->layer.src_type = cur_datatype;
                layer->layer.item_size = hvaultDatatypeSize[cur_datatype];
            }
            /* Check that type is equal to previous files */
            else if (layer->layer.src_type != cur_datatype)
            {
                elog(WARNING, 
                     "Dataset %s type %d is incompatible with prev files %d",
                     layer->dataset_name, cur_datatype, layer->layer.src_type);
                MemoryContextSwitchTo(oldmemctx);
                hvaultGDALCloseLayer(layer);
                continue;
            }
        
            /* Get fill value */
            if (layer->layer.src_type != HvaultBitmap && 
                layer->layer.src_type != HvaultPrefixBitmap)
            {
                int fill_value_res;
                double fill_val = 
                    GDALGetRasterNoDataValue(layer->band, &fill_value_res);
                if (fill_value_res) 
                {
                    if (layer->layer.fill_val == NULL)
                    {
                        layer->layer.fill_val = palloc(layer->layer.item_size);
                    }

                    doubleToType(fill_val, layer->layer.src_type, 
                                 layer->layer.fill_val);
                }
            }

            /* Get range, scale and offset */
            if (layer->coltypid == FLOAT8OID)
            {
                int res;
                double scale, offset;
                
                layer->layer.scale = 1.;
                scale = GDALGetRasterScale(layer->band, &res);
                if (res)
                    layer->layer.scale = scale;

                layer->layer.offset = 0;
                offset = GDALGetRasterOffset(layer->band, &res);
                if (res)
                    layer->layer.offset = offset;
                
                if (layer->flags & FLAG_INVERSE_SCALE)
                {
                    double new_scale = 1.0 / layer->layer.scale;
                    double new_offset = 
                        -layer->layer.offset * layer->layer.scale;
                    layer->layer.scale = new_scale;
                    layer->layer.offset = new_offset;
                }
                /* TODO: range support */
            }

            if (layer->layer.data == NULL)
            {
                layer->layer.data = palloc(layer->layer.item_size *
                                           layer->num_samples * 
                                           layer->num_lines);
            }
        } /* end layer loop */

        /* Sanity check */
        if (driver->num_lines == 0 || driver->num_samples == 0)
        {
            elog(WARNING, 
                 "Can't get number of lines and samples, skipping record");
            MemoryContextSwitchTo(oldmemctx);
            hvaultGDALClose(drv);
            return;
        }
        /* Check that geolocation is available */
        if (driver->flags & (FLAG_HAS_POINT | FLAG_HAS_FOOTPRINT))
        {
            HvaultCatalogItem const * tile_item;

            if (driver->transform == NULL)
            {
                elog(WARNING, 
                     "Can't get geolocation transform, skipping record");
                MemoryContextSwitchTo(oldmemctx);
                hvaultGDALClose(drv);
                return;
            }

            /* Initialize cache and temp geo buffers */
            /* TODO: maybe it's better to do this during driver initialization */
            if (driver->geo_cache == NULL)
            {
                driver->geo_cache = createGeolocation(driver);
                driver->geo_cache_last = driver->geo_cache;
                driver->geo_cache->next = driver->geo_cache;

                driver->lat_temp = palloc(sizeof(double) 
                                          * (driver->num_samples + 1) 
                                          * (driver->num_lines + 1));
                driver->lon_temp = palloc(sizeof(double) 
                                          * (driver->num_samples + 1) 
                                          * (driver->num_lines + 1));
            }
            
            HASH_FIND_STR(products, driver->tile_col, tile_item);
            driver->tile = tile_item != NULL ? tile_item->str : NULL;
        }

        driver->read_complete = false;
        MemoryContextSwitchTo(oldmemctx);
    }
    PG_CATCH();
    {
        if (oldmemctx != NULL)
            MemoryContextSwitchTo(oldmemctx);

        hvaultGDALFree(drv);
        PG_RE_THROW();
    }
    PG_END_TRY();
}


static void
geoTransform(OGRCoordinateTransformationH transform,
             size_t num_points, bool shift_longitude,
             double * lat_temp, double * lon_temp, 
             float * lat_data, float * lon_data)
{
    size_t i;

    Assert(transform != NULL);
    Assert(lat_temp != NULL);
    Assert(lon_temp != NULL);
    Assert(lat_data != NULL);
    Assert(lon_data != NULL);

    if(OCTTransform(transform, num_points, lon_temp, lat_temp, NULL) == FALSE) {
        elog(ERROR, "Can't perform coordinate transformation");
        return; /* Will never reach this */
    }

    for (i = 0; i < num_points; i++)
    {
        lat_data[i] = lat_temp[i];
        lon_data[i] = lon_temp[i];
    }
    
    if (shift_longitude)
    {
        for (i = 0; i < num_points; i++)
        {
            if (lon_data[i] < 0) 
                lon_data[i] += 360;
        }
    }
}

static HvaultGDALGeolocation * 
getGeolocation (HvaultGDALDriver * driver,
                char const * tile)
{
    size_t i, j, num_points;
    size_t cur_num = 0;
    HvaultGDALGeolocation *cur, *prev;
    MemoryContext oldmemctx;

    Assert(driver->geo_cache != NULL);
    Assert(driver->geo_cache_last != NULL);
        
    oldmemctx = MemoryContextSwitchTo(driver->memctx);
    
    if (tile != NULL)
    {
        elog(DEBUG1, "hvault: searching cache for geolocation %s", tile);
        
        /* Search cache for tile */
        cur = driver->geo_cache;
        if (cur->tile != NULL && strcmp(cur->tile, tile) == 0) 
        {
            elog(DEBUG1, "hvault: found geolocation in cache first");
            MemoryContextSwitchTo(oldmemctx);
            return cur;
        } 
        else 
        {
            while (cur->next != driver->geo_cache_last) 
            {
                prev = cur;
                cur = cur->next;
                cur_num++;

                if (cur->tile != NULL && strcmp(cur->tile, tile) == 0)
                {
                    /* Move to first position */
                    prev->next = cur->next;
                    cur->next = driver->geo_cache;
                    driver->geo_cache = cur;
                    driver->geo_cache_last->next = cur;

                    elog(DEBUG1, "hvault: found geolocation in cache");
                    MemoryContextSwitchTo(oldmemctx);
                    return cur;
                }
            }

            prev = cur;
            cur = cur->next;
            if (cur->tile != NULL && strcmp(cur->tile, tile) == 0)
            {
                /* Move to first position */
                driver->geo_cache = cur;
                driver->geo_cache_last = prev;
                    
                elog(DEBUG1, "hvault: found geolocation in cache last");
                MemoryContextSwitchTo(oldmemctx);
                return cur;
            }
        }

        /* Not found in cache. Create new bucket or reuse one */
        if (cur_num < driver->geo_cache_size && cur->tile != NULL)
        {
            elog(DEBUG1, "hvault: creating new bucket for geolocation");
            /* Create new bucket */
            cur = createGeolocation(driver);
            
            /* Set as first item */
            cur->next = driver->geo_cache;
            driver->geo_cache = cur;
            driver->geo_cache_last->next = cur;
        }
        else 
        {
            elog(DEBUG1, "hvault: reusing bucket for geolocation");

            /* Pick up last one (cur already points at it)*/
            Assert(driver->geo_cache_last == cur);
            
            /* Set as first item */
            driver->geo_cache = cur;
            driver->geo_cache_last = prev;
        }

        if (cur->tile != NULL)
            pfree((void *) cur->tile);
        cur->tile = pstrdup(tile);
    } 
    else
    {
        elog(DEBUG1, "hvault: can't search geolocation cache");
        /* Can't search in cache, just reuse last bucket without moving it*/
        cur = driver->geo_cache_last;
        if (cur->tile != NULL)
            pfree((void *) cur->tile);
        cur->tile = NULL;
    }

    Assert(driver->lon_temp != NULL);
    Assert(driver->lat_temp != NULL);

    if (driver->flags & FLAG_HAS_FOOTPRINT)
    {
        for (i = 0; i <= driver->num_lines; i++)
        {
            for (j = 0; j <= driver->num_samples; j++)
            {
                driver->lon_temp[i * (driver->num_samples + 1) + j] = 
                    driver->aft[0] + driver->aft[1] * j + driver->aft[2] * i;
                driver->lat_temp[i * (driver->num_samples + 1) + j] = 
                    driver->aft[3] + driver->aft[4] * j + driver->aft[5] * i;
            }
        }
        num_points = (driver->num_lines + 1) * (driver->num_samples + 1);

        geoTransform(driver->transform, num_points, 
                     driver->flags & FLAG_SHIFT_LONGITUDE,
                     driver->lat_temp, driver->lon_temp,
                     cur->fp_lat, cur->fp_lon);
    }

    if (driver->flags & FLAG_HAS_POINT)
    {
        for (i = 0; i < driver->num_lines; i++)
        {
            for (j = 0; j < driver->num_samples; j++)
            {
                driver->lon_temp[i * driver->num_samples + j] = 
                    driver->aft[0] + driver->aft[1] * (0.5 + j) 
                                   + driver->aft[2] * (0.5 + i);
                driver->lat_temp[i * driver->num_samples + j] = 
                    driver->aft[3] + driver->aft[4] * (0.5 + j) 
                                   + driver->aft[5] * (0.5 + i);
            }
        }
        num_points = driver->num_lines * driver->num_samples;

        geoTransform(driver->transform, num_points, 
                     driver->flags & FLAG_SHIFT_LONGITUDE,
                     driver->lat_temp, driver->lon_temp,
                     cur->point_lat, cur->point_lon);
    }

    MemoryContextSwitchTo(oldmemctx);
    return cur;
}


static void 
readChunk (HvaultGDALDriver * driver,
           HvaultFileChunk  * chunk)
{
    ListCell *l;
    
    chunk->const_layers = NIL;
    chunk->layers = NIL;
    chunk->stride = driver->num_samples;
    chunk->size = driver->num_samples * driver->num_lines;

    foreach(l, driver->layers)
    {
        HvaultGDALLayer *layer = lfirst(l);
        CPLErr res;
        if (layer->band == NULL)
            continue;

        res = GDALRasterIO(layer->band, GF_Read, 0, 0, 
                           layer->num_lines, layer->num_samples,
                           layer->layer.data, 
                           layer->num_lines, layer->num_samples,
                           layer->gdal_type, 0, 0);
        if (res != CE_None)
        {
            elog(ERROR, "Can't read data from %s", layer->dataset_name);
            return; /* will never reach here */
        }

        /* FIXME: Not sure about clause */
        if (layer->layer.colnum >= 0)
            chunk->layers = lappend(chunk->layers, layer);
    }
    
    if (driver->flags & (FLAG_HAS_FOOTPRINT | FLAG_HAS_POINT))
    {
        HvaultGDALGeolocation * loc;
        
        loc = getGeolocation(driver, driver->tile);
        chunk->lat = loc->fp_lat;
        chunk->lon = loc->fp_lon;
        chunk->point_lat = loc->point_lat;
        chunk->point_lon = loc->point_lon;
    }

    driver->read_complete = true;
}

static void 
hvaultGDALRead (HvaultFileDriver * drv,
                HvaultFileChunk  * chunk)
{
    MemoryContext oldmemctx = NULL;
    PG_TRY();
    {
        HvaultGDALDriver * driver = (HvaultGDALDriver *) drv;

        Assert(driver->driver.methods == &hvaultGDALMethods);
        MemoryContextReset(driver->chunkmemctx);
        oldmemctx = MemoryContextSwitchTo(driver->chunkmemctx);

        if (driver->read_complete)
        {
            chunk->size = 0;
        }
        else
        {
            readChunk(driver, chunk);
        }
        
        MemoryContextSwitchTo(oldmemctx);
    }
    PG_CATCH();
    {
        if (oldmemctx != NULL)
            MemoryContextSwitchTo(oldmemctx);

        hvaultGDALFree(drv);
        PG_RE_THROW();
    }
    PG_END_TRY();
}


const HvaultFileDriverMethods hvaultGDALMethods = 
{
    hvaultGDALInit,
    hvaultGDALAddColumn,
    hvaultGDALOpen,
    hvaultGDALRead,
    hvaultGDALClose,
    hvaultGDALFree
};
