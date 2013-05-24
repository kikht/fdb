#ifndef _ANALYZE_H_
#define _ANALYZE_H_

#include "common.h"
#include <optimizer/restrictinfo.h>

typedef enum {
    HvaultQualDefault,
    HvaultQualSimple,
    HvaultQualGeom
} HvaultQualType;

/* Base struct for hvault qual data */
typedef struct {
    HvaultQualType type;
    RestrictInfo * rinfo;
    /* private data */
} HvaultQual;

typedef struct HvaultQualAnalyzerData * HvaultQualAnalyzer;


/* Initialize analyzer. Reads geometry oper oids */
HvaultQualAnalyzer HvaultAnalyzerInit (HvaultTableInfo const * table);

/* Free analyer resources */
void HvaultAnalyzerFree (HvaultQualAnalyzer analyzer);

/* Process one restrict info */
HvaultQual HvaultAnalyzerProcess (HvaultQualAnalyzer analyzer, 
                                  RestrictInfo *     rinfo);

// bool isCatalogQual (Expr *expr, HvaultTableInfo const *table);

// Var * isCatalogJoinEC(EquivalenceClass *ec, HvaultTableInfo const *table);

// bool isFootprintQual (Expr *expr, 
//                       const HvaultTableInfo *table, 
//                       HvaultGeomOpQual qual);

// void deparseFootprintExpr (HvaultGeomOpQual qual, 
//                            HvaultDeparseContext *ctx);
#endif
