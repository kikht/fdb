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

#include "common.h"
#include "analyze.h"

typedef struct
{
    StringInfoData query;
    List *fdw_expr;
    HvaultTableInfo const *table;
} HvaultDeparseContext;

/* Initializes HvaultDeparseContext struct (with previously undefined contents)
 * to describe empty qual list.
 */
void hvaultDeparseContextInit (HvaultDeparseContext  * ctx, 
                               HvaultTableInfo const * table);

/* Frees all resources allocated by context. It's up to caller to pfree 
 * the struct itself (so it is possible to allocate struct on stack, 
 * or reinit this context later)
 */
void hvaultDeparseContextFree (HvaultDeparseContext * ctx);

/* Deparse simple catalog expression (file_id and starttime vars only) */
void hvaultDeparseSimple (Expr *node, HvaultDeparseContext *ctx);

/* Deparse footprint catalog expression */
void hvaultDeparseFootprint (HvaultGeomOperator     op, 
                             bool                   isneg, 
                             Expr *                 arg, 
                             HvaultDeparseContext * ctx);

/* Deparse qual into catalog query string. 
 * This function is entry point to different deparse strategies.
 * It is implemented in analyze.c, that incapsulates knowledge about 
 * different HvaultQuals
 */
void hvaultDeparseQual (HvaultQual * qual, HvaultDeparseContext * ctx);

#endif
