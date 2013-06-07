#ifndef _CATALOG_H_
#define _CATALOG_H_

#include "common.h"
#include "analyze.h"

typedef struct HvaultCatalogQueryData * HvaultCatalogQuery;
typedef struct HvaultCatalogCursorData * HvaultCatalogCursor;
typedef enum {
    HvaultCatalogCursorOK,
    HvaultCatalogCursorNotStarted,
    HvaultCatalogCursorEOF,
    HvaultCatalogCursorError
} HvaultCatalogCursorResult;

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
void hvaultCatalogAddProduct (HvaultCatalogQuery query, char const * name);

/* Add catalog qual to query */
void hvaultCatalogAddQual (HvaultCatalogQuery query,
                           HvaultQual *       qual);

/* Add sort qual to query */
void hvaultCatalogSetSort (HvaultCatalogQuery query, 
                           char const *       qual);

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

/* Gets current record's id */
int hvaultCatalogGetId (HvaultCatalogCursor cursor);

/* Get current records's start time */
Timestamp hvaultCatalogGetStarttime (HvaultCatalogCursor cursor);

/* Get current records's stop time */
Timestamp hvaultCatalogGetStoptime (HvaultCatalogCursor cursor);

/* Get current record's product filename */
char const * hvaultCatalogGetFilename (HvaultCatalogCursor cursor,
                                       char const *        product);


/*
 * Other functions
 */

double hvaultGetNumFiles(char const *catalog);


#endif
