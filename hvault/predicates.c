#include "hdf.h"
#include "predicates.h"

static inline bool 
Overlaps_op(float latmin, 
            float latmax, 
            float lonmin, 
            float lonmax, 
            GBOX const *arg)
{
    return !( (latmin > arg->xmax) || (arg->xmin > latmax) ||
              (lonmin > arg->ymax) || (arg->ymin > lonmax) );
}

static inline bool 
Contains_op(float latmin, 
            float latmax, 
            float lonmin, 
            float lonmax, 
            GBOX const *arg)
{
    return !( (latmin > arg->xmin) || (latmax < arg->xmax) ||
              (lonmin > arg->ymin) || (lonmax < arg->ymax) );
}

static inline bool 
Within_op(float latmin, 
          float latmax, 
          float lonmin, 
          float lonmax, 
          GBOX const *arg)
{
    return !( (arg->xmin > latmin) || (arg->xmax < latmax) ||
              (arg->ymin > lonmin) || (arg->ymax < lonmax) );
}

static inline bool 
Same_op(float latmin, 
          float latmax, 
          float lonmin, 
          float lonmax, 
          GBOX const *arg)
{
    return !( (latmin != arg->xmin) || (latmax != arg->xmax) ||
              (lonmin != arg->ymin) || (lonmax != arg->ymax) );
}

static inline bool 
Overleft_op(float latmin, 
            float latmax, 
            float lonmin, 
            float lonmax, 
            GBOX const *arg)
{
    return latmax <= arg->xmax;
}

static inline bool 
Left_op(float latmin, 
        float latmax, 
        float lonmin, 
        float lonmax, 
        GBOX const *arg)
{
    return latmax < arg->xmin;
}

static inline bool 
Right_op(float latmin, 
         float latmax, 
         float lonmin, 
         float lonmax, 
         GBOX const *arg)
{
    return latmin > arg->xmax;
}

static inline bool 
Overright_op(float latmin, 
             float latmax, 
             float lonmin, 
             float lonmax, 
             GBOX const *arg)
{
    return latmin >= arg->xmin;
}

static inline bool 
Overbelow_op(float latmin, 
             float latmax, 
             float lonmin, 
             float lonmax, 
             GBOX const *arg)
{
    return lonmax <= arg->ymax;
}

static inline bool 
Below_op(float latmin, 
         float latmax, 
         float lonmin, 
         float lonmax, 
         GBOX const *arg)
{
    return lonmax < arg->ymin;
}

static inline bool 
Above_op(float latmin, 
         float latmax, 
         float lonmin, 
         float lonmax, 
         GBOX const *arg)
{
    return lonmin > arg->ymax;
}

static inline bool 
Overabove_op(float latmin, 
             float latmax, 
             float lonmin, 
             float lonmax, 
             GBOX const *arg)
{
    return lonmin >= arg->ymin;
}

static inline bool 
CommLeft_op(float latmin, 
            float latmax, 
            float lonmin, 
            float lonmax, 
            GBOX const *arg)
{
    return latmax >= arg->xmax;
}

static inline bool 
CommRight_op(float latmin, 
             float latmax, 
             float lonmin, 
             float lonmax, 
             GBOX const *arg)
{
    return latmin <= arg->xmin;
}

static inline bool 
CommBelow_op(float latmin, 
             float latmax, 
             float lonmin, 
             float lonmax, 
             GBOX const *arg)
{
    return lonmax >= arg->ymax;
}

static inline bool 
CommAbove_op(float latmin, 
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
} while(0)

#define compact_bounds \
{ \
    float ul, ur, lr, ll; \
  \
    ul = data->lat[cur]; \
    ur = data->lat[cur + 1]; \
    lr = data->lat[cur + data->stride + 1]; \
    ll = data->lat[cur + data->stride]; \
    bounds(ul, ur, lr, ll, latmin, latmax); \
 \
    ul = data->lon[cur]; \
    ur = data->lon[cur + 1]; \
    lr = data->lon[cur + data->stride + 1]; \
    ll = data->lon[cur + data->stride]; \
    bounds(ul, ur, lr, ll, lonmin, lonmax); \
} while(0)

#define simple_bounds \
{ \
    float ul, ur, lr, ll; \
  \
    ul = data->lat[4*cur + 0]; \
    ur = data->lat[4*cur + 1]; \
    lr = data->lat[4*cur + 2]; \
    ll = data->lat[4*cur + 3]; \
    bounds(ul, ur, lr, ll, latmin, latmax); \
  \
    ul = data->lon[4*cur + 0]; \
    ur = data->lon[4*cur + 1]; \
    lr = data->lon[4*cur + 2]; \
    ll = data->lon[4*cur + 3]; \
    bounds(ul, ur, lr, ll, lonmin, lonmax); \
} while(0)

#define point_bounds \
{ \
    float lat, lon; \
  \
    lat = data->point_lat[cur]; \
    lon = data->point_lon[cur]; \
  \
    latmin = latmax = lat; \
    lonmin = lonmax = lon; \
} while(0)

#define cur_simple size_t cur = i
#define cur_indexed size_t cur = idx[i]

#define predicate_cycle(cur, my_bounds, op, neg) \
{ \
    for (i = 0, j = 0; i < len; i++) \
    { \
        float latmin, latmax, lonmin, lonmax; \
        cur; \
        my_bounds; \
        if (op(latmin, latmax, lonmin, lonmax, arg) ^ (neg)) \
            idx[j++] = i; \
    } \
} while(0)

#define positive false 
#define negative true

#define predicate_template(src_type, op, neg) \
static size_t op ## _ ## src_type ## _ ## neg (  \
    size_t * idx,  \
    size_t len,  \
    HvaultFileChunk const * const data, \
    GBOX const * const arg) \
{ \
    size_t i, j; \
    if (len == data->size) \
    { \
        /* Use full dataset */ \
        predicate_cycle(cur_simple, src_type ## _bounds, op ## _op, neg); \
    } \
    else \
    { \
        /* Use selected indices */ \
        predicate_cycle(cur_indexed, src_type ## _bounds, op ## _op, neg); \
    } \
    return j; \
} 

#define predicate_multitemplate(op) \
    predicate_template(simple, op, positive) \
    predicate_template(simple, op, negative) \
    predicate_template(compact, op, positive) \
    predicate_template(compact, op, negative) \
    predicate_template(point, op, positive) \
    predicate_template(point, op, negative) 

#define operators_list(item) \
    item(Overlaps) \
    item(Contains) \
    item(Within) \
    item(Same) \
    item(Overleft) \
    item(Overright) \
    item(Overabove) \
    item(Overbelow) \
    item(Left) \
    item(Right) \
    item(Above) \
    item(Below) \
    item(CommLeft) \
    item(CommRight) \
    item(CommAbove) \
    item(CommBelow) 

operators_list(predicate_multitemplate)

#define predicate_name(op, src_type) \
    op ## _ ## src_type ## _ ## positive, \
    op ## _ ## src_type ## _ ## negative, 

#define point_predicate_name(op) predicate_name(op, point)
HvaultPredicate hvaultPredicatesPoint[HvaultGeomNumAllOpers * 2] = 
    { operators_list(point_predicate_name) };
    
#define simple_predicate_name(op) predicate_name(op, simple)
HvaultPredicate hvaultPredicatesSimple[HvaultGeomNumAllOpers * 2] = 
    { operators_list(simple_predicate_name) };

#define compact_predicate_name(op) predicate_name(op, compact)
HvaultPredicate hvaultPredicatesCompact[HvaultGeomNumAllOpers * 2] = 
    { operators_list(compact_predicate_name) };
