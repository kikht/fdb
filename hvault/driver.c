#include "driver.h"
#include "options.h"

extern const HvaultFileDriverMethods hvaultModisSwathMethods;
extern const HvaultFileDriverMethods hvaultGDALMethods;

HvaultFileDriver * 
hvaultGetDriver (List *table_options, MemoryContext memctx)
{
    char const * driver_str;

    driver_str = defFindStringByName(table_options, HVAULT_TABLE_OPTION_DRIVER);
    if (driver_str == NULL)
        elog(ERROR, "Must specify driver for hvault table");

    if (strcmp(driver_str, "modis_swath") == 0)
        return hvaultModisSwathMethods.init(table_options, memctx);
    else if (strcmp(driver_str, "gdal") == 0)
        return hvaultGDALMethods.init(table_options, memctx);
    else
    {
        elog(ERROR, "Unknown driver: %s", driver_str);
        return NULL; /* Will never reach this */
    }
}
