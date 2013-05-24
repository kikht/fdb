/* -----------------------------------
 *
 * Deparse expression functions
 *
 * These functions examine query clauses and translate them to corresponding 
 * catalog query clauses. It is intended to reduce number of files we need to 
 * scan for the query. Lots of deparse functions are ported from 
 * contrib/postgres_fdw/deparse.c and ruleutils.c. It seems that deparsing 
 * API should be extended somehow to handle such cases.
 *
 * -----------------------------------
 */

#ifndef _DEPARSE_H_
#define _DEPARSE_H_

#include <postgres.h>
#include <nodes/primnodes.h>
#include <lib/stringinfo.h>
#include "common.h"

typedef struct
{
    StringInfoData query;
    List *fdw_expr;
    HvaultTableInfo *table;
} HvaultDeparseContext;

void deparseExpr(Expr *node, HvaultDeparseContext *ctx);

#endif
