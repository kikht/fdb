#ifndef _DRIVER_H_
#define _DRIVER_H_

#include "utils.h"
#include "catalog.h"

typedef struct HvaultFileDriver HvaultFileDriver;
typedef struct HvaultFileChunk HvaultFileChunk;

typedef struct 
{
    HvaultFileDriver * (* init) (List        * table_options, 
                                 MemoryContext memctx);
    void (* add_column) (HvaultFileDriver        * driver,
                         Form_pg_attribute         attr,
                         List                    * options);
    void (* open      ) (HvaultFileDriver        * driver, 
                         HvaultCatalogItem const * products);
    void (* read      ) (HvaultFileDriver        * driver,
                         HvaultFileChunk         * chunk);
    void (* close     ) (HvaultFileDriver        * driver);
    void (* free      ) (HvaultFileDriver        * driver);
} HvaultFileDriverMethods;

typedef enum 
{
    HvaultGeolocationCompact,
    HvaultGeolocationSimple
} HvaultGeolocationType;

typedef enum  
{
    HvaultInvalidLayerType = -1,

    HvaultLayerConst = 0,
    HvaultLayerSimple,
    HvaultLayerChunked,

    HvaultLayerNumTypes
} HvaultLayerType;

typedef struct HvaultFileLayer
{
    void * data;
    void * fill_val;
    void * temp;
    double scale, offset;

    AttrNumber colnum;
    HvaultLayerType type;
    HvaultDataType src_type;
    size_t item_size;
    int hfactor, vfactor;
} HvaultFileLayer;

struct HvaultFileChunk 
{
    List * const_layers;
    List * layers; 

    float * lat;
    float * lon;

    float * point_lat;
    float * point_lon;

    size_t size, stride;
};

struct HvaultFileDriver
{
    HvaultFileDriverMethods const * methods;
    HvaultGeolocationType geotype;
};

HvaultFileDriver * hvaultGetDriver (List *table_options, MemoryContext memctx);


#endif
