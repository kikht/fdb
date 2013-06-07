#ifndef _COMMON_H_
#define _COMMON_H_

#include <postgres.h>
#include <access/htup.h>
#include <access/skey.h>
#include <catalog/namespace.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <commands/explain.h>
#include <commands/vacuum.h>
#include <executor/spi.h>
#include <foreign/fdwapi.h>
#include <foreign/foreign.h>
#include <nodes/bitmapset.h>
#include <nodes/nodeFuncs.h>
#include <nodes/primnodes.h>
#include <optimizer/cost.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/planmain.h>
#include <optimizer/restrictinfo.h>
#include <postgres_ext.h>
#include <tcop/tcopprot.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/rel.h>
#include <utils/syscache.h>
#include <utils/timestamp.h>

#define HVAULT_TUPLES_PER_FILE (double)(2030*1354)

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

extern char const * hvaultGeomopstr[HvaultGeomNumAllOpers];

typedef struct 
{
    Index relid;      
    int natts;
    HvaultColumnType *coltypes;
    char const * catalog;
} HvaultTableInfo;

#endif
