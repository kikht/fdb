#ifndef _CATALOG_H_
#define _CATALOG_H_

#include "common.h"
#include "utils.h"
#include "analyze.h"

typedef struct HvaultCatalogQueryData * HvaultCatalogQuery;
typedef struct HvaultCatalogCursorData * HvaultCatalogCursor;
typedef enum {
    HvaultCatalogCursorOK,
    HvaultCatalogCursorNotStarted,
    HvaultCatalogCursorEOF,
    HvaultCatalogCursorError
} HvaultCatalogCursorResult;

typedef struct {
    char * name;
    char * str;
    Datum  val;
    Oid    typid;
    UT_hash_handle hh;
} HvaultCatalogItem;


/*
 * Query construction routines
 */

/* Creates new catalog query builder and initialize it*/
HvaultCatalogQuery hvaultCatalogInitQuery (HvaultTableInfo const * table);

/* Creates deep copy of a query */
HvaultCatalogQuery hvaultCatalogCloneQuery (HvaultCatalogQuery query);

/* Frees builder and all it's data */
void hvaultCatalogFreeQuery (HvaultCatalogQuery query);

/* Add required product to query */
void hvaultCatalogAddColumn (HvaultCatalogQuery query, char const * name);

/* Add catalog qual to query */
void hvaultCatalogAddQual (HvaultCatalogQuery query,
                           HvaultQual *       qual);

/* Reset sort quals in query */
void hvaultCatalogResetSort (HvaultCatalogQuery query);

/* Add sort qual to query */
void hvaultCatalogAddSort (HvaultCatalogQuery query, 
                           char const *       qual,
                           bool               desc);

/* Add limit qual to query */
void hvaultCatalogSetLimit (HvaultCatalogQuery query, 
                            size_t             limit);

/* Get list of required parameter expressions that need to be evaluated 
   and passed to query cursor */
List * hvaultCatalogGetParams (HvaultCatalogQuery query);

/* Get cost estimate of catalog query */
void hvaultCatalogGetCosts(HvaultCatalogQuery query,
                           Cost *             startup_cost,
                           Cost *             total_cost,
                           double *           plan_rows,
                           int *              plan_width);

/* Pack query to form that postgres knows how to copy */
List * hvaultCatalogPackQuery(HvaultCatalogQuery query);


/*
 * Catalog cursor routines
 */

/* Creates new catalog cursor and initializes it with packed query */
HvaultCatalogCursor hvaultCatalogInitCursor (List * packed_query,
                                             MemoryContext memctx);

/* Destroys cursor and all it's data */
void hvaultCatalogFreeCursor (HvaultCatalogCursor cursor);

/* Starts cursor with specified parameters */
void hvaultCatalogStartCursor (HvaultCatalogCursor cursor, 
                               Oid *               argtypes,
                               Datum *             argvals,
                               char *              argnulls);

/* Resets cursor. Next invocation of hvaultCatalogNext will return 
   HvaultCatalogCursorNotStarted */
void hvaultCatlogResetCursor (HvaultCatalogCursor cursor);

/* Returns number of parameters in catalog query */
int hvaultCatalogGetNumArgs (HvaultCatalogCursor cursor);

/* Returns catalog cursor's query string */
char const * hvaultCatalogGetQuery (HvaultCatalogCursor cursor);

/* Moves cursor to the next catalog record */
HvaultCatalogCursorResult hvaultCatalogNext (HvaultCatalogCursor cursor);

/* Get current record's values */
HvaultCatalogItem const * hvaultCatalogGetValues (HvaultCatalogCursor cursor);


/*
 * Other functions
 */

double hvaultGetNumFiles(char const *catalog);


#endif
