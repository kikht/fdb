#ifndef _ANALYZE_H_
#define _ANALYZE_H_

#include "common.h"
#include <optimizer/restrictinfo.h>

typedef enum 
{
    HvaultQualSimple,
    HvaultQualGeom
} HvaultQualType;

/* Base struct for hvault qual data */
typedef struct 
{
    HvaultQualType type;
    RestrictInfo * rinfo;
    /* private data */
} HvaultQual;

typedef struct 
{
    EquivalenceClass * ec;
    Var * var;
} HvaultEC;

typedef struct HvaultQualAnalyzerData * HvaultQualAnalyzer;


/* Initialize analyzer. Reads geometry oper oids */
HvaultQualAnalyzer hvaultAnalyzerInit (HvaultTableInfo const * table);

/* Free analyer resources */
void hvaultAnalyzerFree (HvaultQualAnalyzer analyzer);

/* Process list of restrict infos and 
   return list of interesting HvaultQuals */
List * hvaultAnalyzeQuals (HvaultQualAnalyzer analyzer, List * quals);

/* Process list of equivalence classes and 
   return list of interesting HvaultECs */
List * hvaultAnalyzeECs (HvaultQualAnalyzer analyzer, List * ecs);

/* Create predicate data represented as List from qual if possible
   Returns NULL if predicate is not available for this qual */
List * hvaultCreatePredicate (HvaultQual * qual, List * fdw_expr);


// bool isCatalogQual (Expr *expr, HvaultTableInfo const *table);

// Var * isCatalogJoinEC(EquivalenceClass *ec, HvaultTableInfo const *table);

// bool isFootprintQual (Expr *expr, 
//                       const HvaultTableInfo *table, 
//                       HvaultGeomOpQual qual);

// void deparseFootprintExpr (HvaultGeomOpQual qual, 
//                            HvaultDeparseContext *ctx);
#endif
