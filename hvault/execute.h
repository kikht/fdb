#ifndef _EXECUTE_H_
#define _EXECUTE_H_

#include "common.h"
#include "catalog.h"
#include <liblwgeom.h>
#include <time.h>

enum HvaultPlanItems
{
    HvaultPlanCatalogQuery = 0,
    HvaultPlanColtypes,
    HvaultPlanPredicates,

    HvaultPlanNumParams
};

enum HvaultPredicateItems
{
    HvaultPredicateColtype = 0,
    HvaultPredicateGeomOper,
    HvaultPredicateArgno,
    HvaultPredicateIsNegative,

    HvaultPredicateNumParams
};

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
    size_t *sel;
    int sel_size;

    int32_t sd_id;                  /* HDF interface handle */
    int num_samples, num_lines;     /* dimensions of file datasets */
    clock_t open_time;              /* time when file was opened */
} HvaultHDFFile;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct
{
    /* query info */
    AttrNumber natts;    /* Total number of tuple attributes */
    List *coltypes;      /* List of HvaultColumnTypes for each column */
    bool has_footprint;  /* true if query needs footprint calculation */
    bool shift_longitude;/* true if we need to shift longitude to (0,360) */
    int scan_size;       /* Number of lines in one scan */
    List *fdw_expr;      /* List of prepared for computation query expressions*/
    ExprContext *expr_ctx; /* Context for prepared expressions */
    List *predicates;    /* List of HvaultGeomPredicate */

    /* iteration state */
    HvaultCatalogCursor cursor;
    /* file */
    HvaultHDFFile file;           /* current file */
    HvaultSDSBuffer **colbuffer;  /* SDS buffer references for each column */
    HvaultSDSBuffer *lat, *lon;   /* Latitude & longitude SDS buffers for 
                                     footprint calculation */
    Timestamp file_time;          /* File timestamp */
    /* current position */
    int cur_file, cur_line, cur_sample, cur_sel; /* Current position */

    /* tuple values */
    Datum *values;       /* Tuple values */
    bool *nulls;         /* Tuple null flags */ 
    /* by-reference values */
    double *sds_floats;  /* Values of SDS columns */
    int64_t *sds_ints;   /* Values of SDS columns */
    LWPOINT *point;      /* Pixel point value */
    LWPOLY *poly;        /* Pixel footprint value */
    POINTARRAY *ptarray; /* Point array for footprint */
} HvaultExecState;

#endif /* _EXECUTE_H_ */
