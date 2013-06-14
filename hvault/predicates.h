#ifndef _PREDICATES_H_
#define _PREDICATES_H_

#include "driver.h"
#include <liblwgeom.h>

typedef size_t (* HvaultPredicate)(size_t * idx,
                                   size_t len, 
                                   HvaultFileChunk const * data,
                                   GBOX const * arg);

extern HvaultPredicate hvaultPredicatesPoint[HvaultGeomNumAllOpers * 2];
extern HvaultPredicate hvaultPredicatesSimple[HvaultGeomNumAllOpers * 2];
extern HvaultPredicate hvaultPredicatesCompact[HvaultGeomNumAllOpers * 2];

static inline HvaultPredicate 
hvaultGetPredicate (HvaultGeomOperator op, 
                    bool isneg, 
                    HvaultColumnType coltype, 
                    HvaultGeolocationType geotype)
{
    switch (coltype) 
    {
        case HvaultColumnPoint:
            return hvaultPredicatesPoint[op*2 + isneg];
        case HvaultColumnFootprint:
            switch (geotype)
            {
                case HvaultGeolocationSimple:
                    return hvaultPredicatesSimple[op*2 + isneg];
                case HvaultGeolocationCompact:
                    return hvaultPredicatesCompact[op*2 + isneg];
                default:
                    return NULL;
            }
        default:
            return NULL;
    }
}

#endif
