#include "utils.h"
#include <utils/int8.h>

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

char *
defFindStringByName (List *list, const char *key)
{
    DefElem * def = defFindByName(list, key);
    return def == NULL ? NULL : defGetString(def);
}

int64_t 
defGetInt (DefElem * def)
{
    if (def->arg == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("%s requires a numeric value",
                        def->defname)));
    switch (nodeTag(def->arg))
    {
        case T_Integer:
            return (int64) intVal(def->arg);
        case T_Float:
        case T_String:
            /*
             * Values too large for int4 will be represented as Float
             * constants by the lexer.  Accept these if they are valid int8
             * strings.
             */
            return DatumGetInt64(DirectFunctionCall1(int8in,
                                         CStringGetDatum(strVal(def->arg))));
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("%s requires a numeric value",
                            def->defname)));
    }
    return 0;                   /* keep compiler quiet */
}
