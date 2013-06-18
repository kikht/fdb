#ifndef _OPTIONS_H_
#define _OPTIONS_H_

#include "common.h"
#include "utils.h"

HvaultColumnType hvaultGetColumnType (DefElem * def);

static inline DefElem * 
hvaultGetTableOption (Oid foreigntableid, char *option)
{
    ForeignTable *foreigntable = GetForeignTable(foreigntableid);
    return defFindByName(foreigntable->options, option);
}

static inline char * 
hvaultGetTableOptionString (Oid foreigntableid, char *option)
{
    return defGetString(hvaultGetTableOption(foreigntableid, option));
}

static inline bool 
hvaultGetTableOptionBool (Oid foreigntableid, char *option)
{
    return defGetBoolean(hvaultGetTableOption(foreigntableid, option));
}

#endif /* _OPTIONS_H_ */
