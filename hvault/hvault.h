#ifndef _HVAULT_H_
#define _HVAULT_H_

#include <time.h>
/* PostgreSQL */

#include <postgres.h>
#include <funcapi.h>
#include <access/attnum.h>
#include <executor/spi.h>
#include <nodes/pg_list.h>
#include <nodes/relation.h>
#include <utils/palloc.h>

/* PostGIS */
#include <liblwgeom.h>



typedef enum HvaultColumnType
{
    HvaultColumnNull,
    HvaultColumnFloatVal,
    HvaultColumnPoint,
    HvaultColumnFootprint,
    HvaultColumnFileIdx,
    HvaultColumnLineIdx,
    HvaultColumnSampleIdx,
    HvaultColumnTime,
} HvaultColumnType;

typedef enum 
{  
    HvaultGeomIntersect = 0,/* &&  */
    HvaultGeomLeft,         /* &<  */
    HvaultGeomRight,        /* &>  */
    HvaultGeomUp,           /* |&> */
    HvaultGeomDown,         /* &<| */
    HvaultGeomStrictLeft,   /* <<  */
    HvaultGeomStrictRight,  /* >>  */
    HvaultGeomStrictUp,     /* |>> */
    HvaultGeomStrictDown,   /* <<| */
    HvaultGeomContains,     /* ~   */
    HvaultGeomIsContained,  /* @   */
    HvaultGeomSame,         /* ~=  */

    HvaultGeomNumOpers,
} HvaultGeomOperator;

typedef struct 
{
    Index relid;      
    AttrNumber natts;
    HvaultColumnType *coltypes;
    char *catalog;

    Oid geomopers[HvaultGeomNumOpers];
    char *geomopermap[HvaultGeomNumOpers*2];
} HvaultTableInfo;

typedef struct 
{
    HvaultTableInfo const *table;
    List *catalog_quals;
    List *footprint_quals;
} HvaultPathData;

enum HvaultPlanItems
{
    HvaultPlanCatalogQuery = 0,
    HvaultPlanColtypes,

    HvaultPlanNumParams,
};

typedef struct
{
    HvaultTableInfo const *table;
    List *fdw_expr;
    StringInfoData query;
} HvaultDeparseContext;

typedef struct 
{
    char * name;             /* Name of SDS */
    void *cur, *next, *prev; /* Buffers for SDS lines */
    void *fill_val;          /* Fill value */
    int32_t id;              /* HDF SDS interface handle */
    int32_t type;            /* HDF type of data */
    double scale, offset;    /* Scale parameters */
    bool haswindow;          /* Whether prev & next lines are required */
} HvaultSDSBuffer;

typedef struct 
{
    MemoryContext filememcxt;       /* context for file related buffers */
    char const * filename;       
    List *sds;                      /* list of HvaultSDSBuffer */
    float *prevbrdlat, *prevbrdlon; /* Buffers for footprint calculation */
    float *nextbrdlat, *nextbrdlon; /* Buffers for footprint calculation */

    int32_t sd_id;                  /* HDF interface handle */
    int num_samples, num_lines;     /* dimensions of file datasets */
    clock_t open_time;              /* time when file was opened */
} HvaultHDFFile;

typedef struct
{
    MemoryContext cursormemctx;
    char const *catalog;       /* Name of catalog table */
    char const *catalog_query; /* WHERE part of the catalog query */
    List *fdw_expr;      /* List of prepared for computation query expressions*/
    ExprContext *expr_ctx;
    char const *file_cursor_name; /* Name of catalog query cursor */
    SPIPlanPtr prep_stmt;
    Datum *values;
    char *nulls;
} HvaultCatalogCursor;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct
{
    /* query info */
    AttrNumber natts;    /* Total number of tuple attributes */
    List *coltypes;      /* List of HvaultColumnTypes for each column */
    bool has_footprint;  /* true if query needs footprint calculation */
    int scan_size;       /* Number of lines in one scan */

    /* iteration state */
    HvaultCatalogCursor cursor;
    /* file */
    HvaultHDFFile file;           /* current file */
    HvaultSDSBuffer **colbuffer;  /* SDS buffer references for each column */
    HvaultSDSBuffer *lat, *lon;   /* Latitude & longitude SDS buffers for 
                                     footprint calculation */
    Timestamp file_time;          /* File timestamp */
    /* current position */
    int cur_file, cur_line, cur_sample; /* Current position */

    /* tuple values */
    Datum *values;       /* Tuple values */
    bool *nulls;         /* Tuple null flags */ 
    /* by-reference values */
    double *sds_vals;    /* Values of SDS columns */
    LWPOINT *point;      /* Pixel point value */
    LWPOLY *poly;        /* Pixel footprint value */
    POINTARRAY *ptarray; /* Point array for footprint */
} HvaultExecState;

/* hvault.c */
extern Datum hvault_fdw_validator(PG_FUNCTION_ARGS);
extern Datum hvault_fdw_handler(PG_FUNCTION_ARGS);
char *get_table_option(Oid foreigntableid, char *option);


/* interpolate.c */
void interpolate_line(size_t m, float const *p, float const *n, float *r);
void extrapolate_line(size_t m, float const *p, float const *n, float *r);

/* plan.c */
void hvaultGetRelSize(PlannerInfo *root, 
                      RelOptInfo *baserel, 
                      Oid foreigntableid);
void hvaultGetPaths(PlannerInfo *root, 
                    RelOptInfo *baserel,
                    Oid foreigntableid);
ForeignScan *hvaultGetPlan(PlannerInfo *root, 
                           RelOptInfo *baserel,
                           Oid foreigntableid, 
                           ForeignPath *best_path,
                           List *tlist, 
                           List *scan_clauses);

#endif /* _HVAULT_H_ */
