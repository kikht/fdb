#include "hvault.h"
#include "hdf.h"

static inline bool 
overlaps_op(float latmin, 
            float latmax, 
            float lonmin, 
            float lonmax, 
            GBOX const *arg)
{
    return !( (latmin > arg->xmax) || (arg->xmin > latmax) ||
              (lonmin > arg->ymax) || (arg->ymin > lonmax) );
}

static inline bool 
contains_op(float latmin, 
            float latmax, 
            float lonmin, 
            float lonmax, 
            GBOX const *arg)
{
    return !( (latmin > arg->xmin) || (latmax < arg->xmax) ||
              (lonmin > arg->ymin) || (lonmax < arg->ymax) );
}

static inline bool 
within_op(float latmin, 
          float latmax, 
          float lonmin, 
          float lonmax, 
          GBOX const *arg)
{
    return !( (arg->xmin > latmin) || (arg->xmax < latmax) ||
              (arg->ymin > lonmin) || (arg->ymax < lonmax) );
}

static inline bool 
equals_op(float latmin, 
          float latmax, 
          float lonmin, 
          float lonmax, 
          GBOX const *arg)
{
    return !( (latmin != arg->xmin) || (latmax != arg->xmax) ||
              (lonmin != arg->ymin) || (lonmax != arg->ymax) );
}

static inline bool 
overleft_op(float latmin, 
            float latmax, 
            float lonmin, 
            float lonmax, 
            GBOX const *arg)
{
    return latmax <= arg->xmax;
}

static inline bool 
left_op(float latmin, 
        float latmax, 
        float lonmin, 
        float lonmax, 
        GBOX const *arg)
{
    return latmax < arg->xmin;
}

static inline bool 
right_op(float latmin, 
         float latmax, 
         float lonmin, 
         float lonmax, 
         GBOX const *arg)
{
    return latmin > arg->xmax;
}

static inline bool 
overright_op(float latmin, 
             float latmax, 
             float lonmin, 
             float lonmax, 
             GBOX const *arg)
{
    return latmin >= arg->xmin;
}

static inline bool 
overbelow_op(float latmin, 
             float latmax, 
             float lonmin, 
             float lonmax, 
             GBOX const *arg)
{
    return lonmax <= arg->ymax;
}

static inline bool 
below_op(float latmin, 
         float latmax, 
         float lonmin, 
         float lonmax, 
         GBOX const *arg)
{
    return lonmax < arg->ymin;
}

static inline bool 
above_op(float latmin, 
         float latmax, 
         float lonmin, 
         float lonmax, 
         GBOX const *arg)
{
    return lonmin > arg->ymax;
}

static inline bool 
overabove_op(float latmin, 
             float latmax, 
             float lonmin, 
             float lonmax, 
             GBOX const *arg)
{
    return lonmin >= arg->ymin;
}

static inline bool 
commleft_op(float latmin, 
            float latmax, 
            float lonmin, 
            float lonmax, 
            GBOX const *arg)
{
    return latmax >= arg->xmax;
}

static inline bool 
commright_op(float latmin, 
             float latmax, 
             float lonmin, 
             float lonmax, 
             GBOX const *arg)
{
    return latmin <= arg->xmin;
}

static inline bool 
commbelow_op(float latmin, 
             float latmax, 
             float lonmin, 
             float lonmax, 
             GBOX const *arg)
{
    return lonmax >= arg->ymax;
}

static inline bool 
commabove_op(float latmin, 
             float latmax, 
             float lonmin, 
             float lonmax, 
             GBOX const *arg)
{
    return lonmin <= arg->ymin;
}

typedef bool (*geom_predicate_op)(float latmin,
                                  float latmax,
                                  float lonmin,
                                  float lonmax,
                                  GBOX const *arg);

#define point_predicate_template(op, scan, arg, neg, n, sel, res) \
{ \
    size_t i, j; \
    if (sel == NULL) \
    { \
        for (i = 0, j = 0; i < n; i++) \
        { \
            float lat = ((float *) scan->lat->cur)[i]; \
            float lon = ((float *) scan->lon->cur)[i]; \
            if (op(lat, lat, lon, lon, arg) ^ neg) \
                res[j++] = i; \
        } \
    } \
    else \
    { \
        for (i = 0, j = 0; i < n; i++) \
        { \
            float lat = ((float *) scan->lat->cur)[sel[i]]; \
            float lon = ((float *) scan->lon->cur)[sel[i]]; \
            if (op(lat, lat, lon, lon, arg) ^ neg) \
                res[j++] = sel[i]; \
        } \
    } \
    return j; \
} 

#define min(a, b) ((a < b) ? a : b) 
#define max(a, b) ((a < b) ? b : a) 
#define bounds(a, b, c, d, minval, maxval) \
{ \
    float minab, maxab, mincd, maxcd; \
    minab = min(a, b); \
    maxab = max(a, b); \
    mincd = min(c, d); \
    maxcd = max(c, d); \
    minval = min(minab, mincd); \
    maxval = max(maxab, maxcd); \
}


#define footprint_predicate_template(op, scan, arg, neg, n, sel, res) \
{ \
    size_t i, j; \
    float latmin, latmax, lonmin, lonmax; \
    if (sel == NULL) \
    { \
        for (i = 0, j = 0; i < n; i++) \
        { \
            bounds(scan->file.prevbrdlat[i],  \
                   scan->file.prevbrdlat[i+1], \
                   scan->file.nextbrdlat[i], \
                   scan->file.nextbrdlat[i+1], \
                   latmin, \
                   latmax); \
            bounds(scan->file.prevbrdlon[i],  \
                   scan->file.prevbrdlon[i+1], \
                   scan->file.nextbrdlon[i], \
                   scan->file.nextbrdlon[i+1], \
                   lonmin, \
                   lonmax); \
            if (op(latmin, latmax, lonmin, lonmax, arg) ^ neg) \
                res[j++] = i; \
        } \
    } \
    else \
    { \
        for (i = 0, j = 0; i < n; i++) \
        { \
            bounds(scan->file.prevbrdlat[sel[i]],  \
                   scan->file.prevbrdlat[sel[i]+1], \
                   scan->file.nextbrdlat[sel[i]], \
                   scan->file.nextbrdlat[sel[i]+1], \
                   latmin, \
                   latmax); \
            bounds(scan->file.prevbrdlon[sel[i]],  \
                   scan->file.prevbrdlon[sel[i]+1], \
                   scan->file.nextbrdlon[sel[i]], \
                   scan->file.nextbrdlon[sel[i]+1], \
                   lonmin, \
                   lonmax); \
            if (op(latmin, latmax, lonmin, lonmax, arg) ^ neg) \
                res[j++] = sel[i]; \
        } \
    } \
    return j; \
} 

int
hvaultGeomPredicate(HvaultColumnType coltype,
                    HvaultGeomOperator op,
                    bool neg,
                    HvaultExecState const *scan,
                    GBOX const * arg,
                    int n,
                    size_t const *sel,
                    size_t *res)
{
    switch (coltype)
    {
        case HvaultColumnPoint:
        {
            switch (op)
            {
                case HvaultGeomOverlaps:
                    point_predicate_template(overlaps_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomContains:
                    point_predicate_template(contains_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomWithin:
                    point_predicate_template(within_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomSame:
                    point_predicate_template(equals_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomOverleft:
                    point_predicate_template(overleft_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomOverright:
                    point_predicate_template(overright_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomOverabove:
                    point_predicate_template(overabove_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomOverbelow:
                    point_predicate_template(overbelow_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomLeft:
                    point_predicate_template(left_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomRight:
                    point_predicate_template(right_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomAbove:
                    point_predicate_template(above_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomBelow:
                    point_predicate_template(below_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomCommLeft:
                    point_predicate_template(commleft_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomCommRight:
                    point_predicate_template(commright_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomCommAbove:
                    point_predicate_template(commabove_op,
                                             scan, arg, neg, n, sel, res);
                case HvaultGeomCommBelow:
                    point_predicate_template(commbelow_op,
                                             scan, arg, neg, n, sel, res);
                default:
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                                    errmsg("Unknown geometry operator %d", op)));
                    return -1;
            }
        }
        break;
        case HvaultColumnFootprint:
        {
            switch (op)
            {
                case HvaultGeomOverlaps:
                    footprint_predicate_template(overlaps_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomContains:
                    footprint_predicate_template(contains_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomWithin:
                    footprint_predicate_template(within_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomSame:
                    footprint_predicate_template(equals_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomOverleft:
                    footprint_predicate_template(overleft_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomOverright:
                    footprint_predicate_template(overright_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomOverabove:
                    footprint_predicate_template(overabove_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomOverbelow:
                    footprint_predicate_template(overbelow_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomLeft:
                    footprint_predicate_template(left_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomRight:
                    footprint_predicate_template(right_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomAbove:
                    footprint_predicate_template(above_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomBelow:
                    footprint_predicate_template(below_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomCommLeft:
                    footprint_predicate_template(commleft_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomCommRight:
                    footprint_predicate_template(commright_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomCommAbove:
                    footprint_predicate_template(commabove_op,
                                                 scan, arg, neg, n, sel, res);
                case HvaultGeomCommBelow:
                    footprint_predicate_template(commbelow_op,
                                                 scan, arg, neg, n, sel, res);
                default:
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                                    errmsg("Unknown geometry operator %d", op)));
                    return -1;
            }
        }
        break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Unknown column type %d", coltype)));
            return -1;   
    }
}               

