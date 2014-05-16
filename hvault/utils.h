#ifndef _UTILS_H_
#define _UTILS_H_

#include "common.h"

#undef HASH_FUNCTION
#include "uthash.h"

/* undefine the uthash defaults */
#undef uthash_malloc
#undef uthash_free
#undef uthash_fatal

/* re-define, specifying alternate functions */
#define uthash_malloc(sz) palloc(sz)
#define uthash_free(ptr,sz) pfree(ptr)
#define uthash_fatal(msg) elog(FATAL, msg)

/* Appends value to List if it is not already there and returns inserted 
 * element's position or position of equal element */
int list_append_unique_pos (List ** list, void * item);

/* Checks whether any of the Relids in 'relids_list' is equal to 'relids' */
bool bms_equal_any(Relids relids, List *relids_list);

/* Finds DefElem with specified name in List */
DefElem * defFindByName (List * list, char const * key);

/* Finds DefElem with specified name in List and extracts String from it */
char * defFindStringByName (List * list, char const * key);

/* Extracts int64_t from DefElem converting from string if necessary */
int64_t defGetInt (DefElem * def);

/* Extracts double from DefElem converting from string if necessary */
double defGetDouble (DefElem * def);

#endif /* _UTILS_H_ */
