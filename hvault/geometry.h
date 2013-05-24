#include "common.h"
#include <nodes/relation.h>



typedef struct {
    HvaultGeomOperator op;
    bool isneg;
} HvaultGeomPredicateDesc;

typedef struct {
    RestrictInfo *rinfo;
    Var *var;
    Expr *arg;
    HvaultColumnType coltype;
    HvaultGeomPredicateDesc pred;
    HvaultGeomPredicateDesc catalog_pred;
} HvaultGeomOpQual;
