#ifndef _OPTIONS_H_
#define _OPTIONS_H_

#include "common.h"
#include "utils.h"

#define HVAULT_COLUMN_OPTION_TYPE "type"
#define HVAULT_COLUMN_OPTION_CATNAME "cat_name"
#define HVAULT_COLUMN_OPTION_FACTOR "factor"
#define HVAULT_COLUMN_OPTION_HFACTOR "hfactor"
#define HVAULT_COLUMN_OPTION_VFACTOR "vfactor"
#define HVAULT_COLUMN_OPTION_DATASET "dataset"
#define HVAULT_COLUMN_OPTION_BITMAPDIMS "bitmap_dims"
#define HVAULT_COLUMN_OPTION_BITMAPTYPE "bitmap_type"
#define HVAULT_COLUMN_OPTION_PREFIX "prefix"
#define HVAULT_COLUMN_OPTION_SCALE "scale"
#define HVAULT_COLUMN_OPTION_OFFSET "offset"
#define HVAULT_COLUMN_OPTION_INVERSE_SCALE "inverse_scale"

#define HVAULT_TABLE_OPTION_DRIVER "driver"
#define HVAULT_TABLE_OPTION_SHIFT_LONGITUDE "shift_longitude"
#define HVAULT_TABLE_OPTION_SCANLINE "scanline"

HvaultColumnType hvaultGetColumnType (DefElem * def);

static inline DefElem * 
hvaultGetTableOption (Oid foreigntableid, char const * option)
{
    ForeignTable *foreigntable = GetForeignTable(foreigntableid);
    return defFindByName(foreigntable->options, option);
}

static inline char * 
hvaultGetTableOptionString (Oid foreigntableid, char const * option)
{
    DefElem * def = hvaultGetTableOption(foreigntableid, option);
    return def == NULL ? NULL : defGetString(def);
}

static inline double 
hvaultGetTableOptionDouble (Oid foreigntableid, char const * option, double defval)
{
	DefElem * def = hvaultGetTableOption(foreigntableid, option);
	return def == NULL ? defval : defGetDouble(def);
}

#endif /* _OPTIONS_H_ */
