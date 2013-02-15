/* PostgreSQL */
#include <postgres.h>
#include <access/attnum.h>
#include <commands/explain.h>
#include <foreign/fdwapi.h>
#include <funcapi.h>
#include <nodes/pg_list.h>

/* PostGIS */
#include <liblwgeom.h>

#define TUPLES_PER_FILE (double)(2030*1354)
/* 4 (size) + 3 (srid) + 1 (flags) + 4 (type) + 4 (num points) + 2 * 8 (coord)*/
#define POINT_SIZE 32
/* 4 (size) + 3 (srid) + 1 (flags) + 4 * 4 (bbox) + 4 (type) + 4 (num rings)
 * +  4 (num points) + 5 * 2 * 8 (coords) */
#define FOOTPRINT_SIZE 120

typedef enum HvaultColumnType
{
    HvaultColumnNull,
    HvaultColumnFloatVal,
    HvaultColumnPoint,
    HvaultColumnFootprint,
    HvaultColumnFileIdx,
    HvaultColumnLineIdx,
    HvaultColumnSampleIdx,
    HvaultColumnTime
} HvaultColumnType;

/*
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct
{
    List *coltypes; /* list of HvaultColumnType as int */
    char *catalog; /* name of catalog table */
} HvaultPlanState;

typedef struct 
{
    char * name;
    void *cur, *next, *prev;
    void *fill_val;
    int32_t id, type;
    double scale, offset;
    bool haswindow;
} HvaultSDSBuffer;

typedef struct 
{
    MemoryContext filememcxt;
    char const * filename;
    List *sds; /* list of HvaultSDSBuffer */
    float *prevbrdlat, *prevbrdlon, *nextbrdlat, *nextbrdlon;

    int32_t sd_id;
    int num_samples, num_lines;
    clock_t open_time;
} HvaultHDFFile;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct
{
    AttrNumber natts;
    List *coltypes;
    HvaultSDSBuffer **colbuffer;
    Datum *values;
    bool *nulls;

    char *catalog;
    char const *file_cursor_name;

    HvaultSDSBuffer *lat, *lon;
    HvaultHDFFile file;

    int cur_file, cur_line, cur_sample;
    Timestamp file_time;
    int scan_size;
    bool has_footprint;

    double *sds_vals;
    LWPOINT *point;
    POINTARRAY *ptarray;
    LWPOLY *poly;
} HvaultExecState;


/*
 * SQL functions
 */
extern Datum hvault_fdw_validator(PG_FUNCTION_ARGS);
extern Datum hvault_fdw_handler(PG_FUNCTION_ARGS);

/* 
 * FDW callback routines 
 */
static void 
hvaultGetRelSize(PlannerInfo *root, 
                 RelOptInfo *baserel, 
                 Oid foreigntableid);
static void 
hvaultGetPaths(PlannerInfo *root, 
               RelOptInfo *baserel,
               Oid foreigntableid);
static ForeignScan *
hvaultGetPlan(PlannerInfo *root, 
              RelOptInfo *baserel,
              Oid foreigntableid, 
              ForeignPath *best_path,
              List *tlist, 
              List *scan_clauses);
static void hvaultExplain(ForeignScanState *node, ExplainState *es);
static void hvaultBegin(ForeignScanState *node, int eflags);
static TupleTableSlot *hvaultIterate(ForeignScanState *node);
static void hvaultReScan(ForeignScanState *node);
static void hvaultEnd(ForeignScanState *node);
/*
static bool 
hvaultAnalyze(Relation relation, 
              AcquireSampleRowsFunc *func,
              BlockNumber *totalpages );
*/

/*
 * Footprint interpolation routines
 */
static inline float interpolate_point(float p1, float p2, float p3, float p4);
static inline float extrapolate_point(float n1, float n2, float p1, float p2);
static inline float extrapolate_corner_point(float c, float l, 
                                             float u, float lu);
static void extrapolate_line(size_t m, 
                             float const *p, 
                             float const *n, 
                             float *r);
static void interpolate_line(size_t m, 
                             float const *p, 
                             float const *n, 
                             float *r);
static void calc_next_footprint(HvaultExecState const *scan, 
                                HvaultHDFFile *file);
/*
 * HDF utils
 */
static size_t hdf_sizeof(int32_t type);
static double hdf_value (int32_t type, void *buffer, size_t offset);
static bool   hdf_cmp   (int32_t type, void *buffer, size_t offset, void *val);
static bool   hdf_file_open(HvaultExecState const *scan,
                            char const *filename,
                            HvaultHDFFile *file);
static void   hdf_file_close(HvaultHDFFile *file);

/*
 * Options routines 
 */
static HvaultColumnType parse_column_type(char *type);
static List *get_column_types(PlannerInfo *root, 
                              RelOptInfo *baserel, 
                              Oid foreigntableid);
static void  check_column_types(List *coltypes, TupleDesc tupdesc);
static char *get_column_sds(Oid relid, AttrNumber attnum, TupleDesc tupdesc);
static int   get_row_width(HvaultPlanState *fdw_private);
static char * get_table_option(Oid foreigntableid, char *option);

/* 
 * Tuple fill utilities
 */
static void fill_float_val(HvaultExecState const *scan, AttrNumber attnum);
static void fill_point_val(HvaultExecState const *scan, AttrNumber attnum);
static void fill_footprint_val(HvaultExecState const *scan, AttrNumber attnum);


static List *get_sort_pathkeys(PlannerInfo *root, RelOptInfo *baserel);
static HvaultSDSBuffer *get_sds_buffer(List **buffers, char *name);

static void fetch_next_line(HvaultExecState *scan);
static double get_num_files(HvaultPlanState *fdw_private);
static bool fetch_next_file(HvaultExecState *scan);
