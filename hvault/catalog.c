#include "catalog.h"

struct HvaultCatalogQueryData
{

};



























HvaultCatalogQuery 
hvaultCatalogInitQuery (char *            catalog,
                        HvaultTableInfo * table)
{

}

/* Creates deep copy of a query */
HvaultCatalogQuery hvaultCatalogCloneQuery (HvaultCatalogQuery query);

/* Frees builder and all it's data */
void hvaultCatalogFreeQuery (HvaultCatalogQuery query);

/* Add required product to query */
void hvaultCatalogAddProduct (HvaultCatalogQuery query, char const * name);

/* Add simple catalog qual to query */
void hvaultCatalogAddSimpleQual (HvaultCatalogQuery query, 
                                 Expr *             expr);

/* Add geometry predicate qual to query */
void hvaultCatalogAddGeometryQual (HvaultCatalogQuery query, 
                                   HvaultGeomOpQual   qual);

/* Add sort qual to query */
void hvaultCatalogAddSortQual (HvaultCatalogQuery query, 
                               char const *       qual);

/* Add limit qual to query */
void hvaultCatalogAddLimitQual (HvaultCatalogQuery query, 
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

