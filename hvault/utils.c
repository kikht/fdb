#include "utils.h"

int 
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

bool 
bms_equal_any(Relids relids, List *relids_list)
{
    ListCell   *lc;

    foreach(lc, relids_list)
    {
        if (bms_equal(relids, (Relids) lfirst(lc)))
            return true;
    }
    return false;
}

DefElem *
defFindByName (List * list, char const * key)
{
    ListCell *l;
    foreach(l, list)
    {
        DefElem *opt = lfirst(l);
        if (strcmp(key, opt->defname) == 0)
        {
            return opt;
        }
    }    
    return NULL;
}
