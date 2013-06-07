#include <postgres.h>
#include <nodes/pg_list.h>
#include <nodes/relation.h>

/* Appends value to List if it is not already there and returns inserted 
 * element's position or position of equal element */
int list_append_unique_pos (List ** list, void * item);

/* Checks whether any of the Relids in 'relids_list' is equal to 'relids' */
bool bms_equal_any(Relids relids, List *relids_list);
