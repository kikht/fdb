#ifndef _COMMON_H_
#define _COMMON_H_

#include <postgres.h>
#include <nodes/primnodes.h>
#include <postgres_ext.h>

typedef enum HvaultColumnType
{
    HvaultColumnNull,
    HvaultColumnPoint,
    HvaultColumnFootprint,
    HvaultColumnFileIdx,
    HvaultColumnLineIdx,
    HvaultColumnSampleIdx,
    HvaultColumnTime,
    HvaultColumnFloatVal,
    HvaultColumnInt8Val,
    HvaultColumnInt16Val,
    HvaultColumnInt32Val,
    HvaultColumnInt64Val
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

    HvaultGeomNumAllOpers
} HvaultGeomOperator;

typedef struct 
{
    Index relid;      
    int natts;
    HvaultColumnType *coltypes;
    char const * catalog;
} HvaultTableInfo;

#endif
