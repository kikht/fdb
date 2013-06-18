#ifndef _DRIVER_H_
#define _DRIVER_H_

#include "utils.h"
#include "catalog.h"

typedef struct HvaultFileDriver HvaultFileDriver;
typedef struct HvaultFileChunk HvaultFileChunk;

typedef struct 
{
    /* init routines (hashtables, memctx,...)
       get geotype*/
    HvaultFileDriver * (* init) ();
    void (* add_column) (HvaultFileDriver        * driver,
                         AttrNumber                attno,
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
    HvaultLayerConst,
    HvaultLayerSimple,
    HvaultLayerChunked
} HvaultLayerType;

typedef enum 
{
    HvaultInt8,
    HvaultInt16,
    HvaultInt32,
    HvaultInt64,
    HvaultFloat32,
    HvaultFloat64,

    HvaultBitmap,

    HvaultUInt8,
    HvaultUInt16,
    HvaultUInt32,
    HvaultUInt64,
} HvaultDataType;

typedef struct HvaultFileLayer
{
    void * data;
    void * fill_val;
    void * temp;
    double scale, offset;

    int16_t colnum;
    HvaultLayerType type;
    HvaultDataType src_type;
    size_t item_size;
    size_t hfactor, vfactor;
} HvaultFileLayer;

struct HvaultFileChunk 
{
    List * const_layers;
    List * layers; /* Null-terminated layers array */

    float * lat;
    float * lon;

    float * point_lat;
    float * point_lon;

    size_t size, stride;
};

struct HvaultFileDriver
{
    HvaultFileDriverMethods * methods;
    HvaultGeolocationType geotype;
};

HvaultFileDriver * hvaultGetDriver (List *table_options, MemoryContext memctx);


#endif
