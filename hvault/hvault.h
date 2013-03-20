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

#define HVAULT_TUPLES_PER_FILE (double)(2030*1354)
#define HVAULT_CATALOG_QUERY_PREFIX "SELECT file_id, filename, starttime FROM "

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
    HvaultGeomInvalidOp = -1,

    HvaultGeomOverlaps = 0,/* &&  */
    HvaultGeomContains,    /* ~   */
    HvaultGeomWithin,      /* @   */
    HvaultGeomSame,        /* ~=  */
    HvaultGeomOverleft,    /* &<  */
    HvaultGeomOverright,   /* &>  */
    HvaultGeomOverabove,   /* |&> */
    HvaultGeomOverbelow,   /* &<| */
    HvaultGeomLeft,        /* <<  */
    HvaultGeomRight,       /* >>  */
    HvaultGeomAbove,       /* |>> */
    HvaultGeomBelow,       /* <<| */

    HvaultGeomNumRealOpers,

    /* fake commutators */
    HvaultGeomCommLeft = HvaultGeomNumRealOpers,
    HvaultGeomCommRight,
    HvaultGeomCommAbove,
    HvaultGeomCommBelow,

    HvaultGeomNumAllOpers,
} HvaultGeomOperator;

enum HvaultPlanItems
{
    HvaultPlanCatalogQuery = 0,
    HvaultPlanColtypes,
    HvaultPlanPredicates,

    HvaultPlanNumParams,
};

enum HvaultPredicateItems
{
    HvaultPredicateColtype = 0,
    HvaultPredicateGeomOper,
    HvaultPredicateArgno,
    HvaultPredicateIsNegative,

    HvaultPredicateNumParams,
};

typedef struct 
{
    HvaultColumnType coltype;
    HvaultGeomOperator op;
    int argno;
    bool isneg;
} HvaultGeomPredicate;

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

typedef struct
{
    char const *query;         /* Catalog query string */

    SPIPlanPtr prep_stmt;
    char const *file_cursor_name; /* Name of catalog query cursor */
    Oid *argtypes;
    Datum *argvals;
    char *argnulls;
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
    List *fdw_expr;      /* List of prepared for computation query expressions*/
    ExprContext *expr_ctx; /* Context for prepared expressions */
    List *predicates;    /* List of HvaultGeomPredicate */

    /* iteration state */
    HvaultCatalogCursor *cursor;
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
    double *sds_vals;    /* Values of SDS columns */
    LWPOINT *point;      /* Pixel point value */
    LWPOLY *poly;        /* Pixel footprint value */
    POINTARRAY *ptarray; /* Point array for footprint */
} HvaultExecState;

/* hvault.c */
extern Datum hvault_fdw_validator(PG_FUNCTION_ARGS);
extern Datum hvault_fdw_handler(PG_FUNCTION_ARGS);


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

/* utils.c */
char *hvaultGetTableOption(Oid foreigntableid, char *option);
HvaultColumnType *hvaultGetUsedColumns(PlannerInfo *root, 
                                       RelOptInfo *baserel, 
                                       Oid foreigntableid,
                                       AttrNumber natts);
List *hvaultGetAllColumns(Relation relation);
double hvaultGetNumFiles(char *catalog);

/* predicate.c */
int hvaultGeomPredicate(HvaultColumnType coltype,
                        HvaultGeomOperator op,
                        bool neg,
                        HvaultExecState const *scan,
                        GBOX const * arg,
                        int n,
                        size_t const *sel,
                        size_t *res);

extern char * geomopstr[HvaultGeomNumAllOpers];


#endif /* _HVAULT_H_ */
