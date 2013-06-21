#ifndef _ANALYZE_H_
#define _ANALYZE_H_

#include "common.h"

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
List * hvaultCreatePredicate (HvaultQual * qual, List ** fdw_expr);

/* Unpacks List representation of predicate into separate fields */
void hvaultUnpackPredicate (List * predicate, 
                            HvaultColumnType * coltype, 
                            HvaultGeomOperator * op,
                            AttrNumber * argno,
                            bool * isneg);

/* Walks through expression tree and calls cb on every found Var that has 
 * specified relid */
void hvaultAnalyzeUsedColumns (Node * expr, 
                               Index relid, 
                               void (*cb)(Var *var, void * arg),
                               void * arg);

#endif
