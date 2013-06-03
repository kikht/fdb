#include <postgres.h>
#include <nodes/pg_list.h>
#include <nodes/relation.h>

static inline int 
list_append_unique_pos (List ** list, void * item)
{
    ListCell *l;
    int idx = 0;

    foreach(l, *list)
    {
        if (lfirst(l) == item)
            break;
        idx++;
    }

    if (idx == list_length(*list))
        *list = lappend(*list, item);

    return idx;
}

bool bms_equal_any(Relids relids, List *relids_list);
