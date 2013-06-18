#include "options.h"

HvaultColumnType
hvaultGetColumnType (DefElem * def)
{
    if (def == NULL)
        return HvaultColumnDataset;

    char *type = defGetString(def);
    if (strcmp(type, "point") == 0) 
    {
        return HvaultColumnPoint;
    }
    else if (strcmp(type, "footprint") == 0)
    {
        return HvaultColumnFootprint;   
    }
    else if (strcmp(type, "catalog") == 0) 
    {
        return HvaultColumnCatalog;
    }
    else if (strcmp(type, "index") == 0)
    {
        return HvaultColumnIndex;
    }
    else if (strcmp(type, "line_index") == 0)
    {
        return HvaultColumnLineIdx;
    }
    else if (strcmp(type, "sample_index") == 0)
    {
        return HvaultColumnSampleIdx;
    }
    else if (strcmp(type, "dataset") == 0)
    {
        return HvaultColumnDataset;
    }
    else
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Unknown column type %s", type)));
        return HvaultColumnNull;
    }
}

// List *
// hvaultGetAllColumns(Relation relation)
// {
//     List *result = NIL;
//     AttrNumber attnum, natts;
//     Oid foreigntableid = RelationGetRelid(relation);

//     natts = RelationGetNumberOfAttributes(relation);
//     for (attnum = 0; attnum < natts; ++attnum)
//     {
//         result = lappend_int(result, get_column_type(foreigntableid, attnum+1));
//     }

//     return result;
// }


