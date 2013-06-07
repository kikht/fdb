#ifndef _OPTIONS_H_
#define _OPTIONS_H_

#include "common.h"

HvaultColumnType * hvaultGetUsedColumns (PlannerInfo *root, 
                                         RelOptInfo *baserel, 
                                         Oid foreigntableid,
                                         AttrNumber natts);

DefElem * hvaultGetTableOption (Oid foreigntableid, char *option);

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

List * hvaultGetAllColumns (Relation relation);

#endif /* _OPTIONS_H_ */
