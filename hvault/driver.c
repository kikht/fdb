#include "driver.h"

extern const HvaultFileDriverMethods hvaultModisSwathMethods;

HvaultFileDriver * 
hvaultGetDriver (List *table_options, MemoryContext memctx)
{
    return hvaultModisSwathMethods.init(table_options, memctx);
}
