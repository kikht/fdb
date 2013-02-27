#ifndef _HDF_H_
#define _HDF_H_

#include "hvault.h"
#include <hdf/mfhdf.h>


bool hdf_file_open(HvaultHDFFile *file,
                   char const *filename,
                   bool has_footprint);
void hdf_file_close(HvaultHDFFile *file);


static inline size_t
hdf_sizeof(int32_t type)
{
    switch(type)
    {
        case DFNT_CHAR8:
        case DFNT_UCHAR8:
        case DFNT_INT8:
        case DFNT_UINT8:
            return 1;
        case DFNT_INT16:
        case DFNT_UINT16:
            return 2;
        case DFNT_INT32:
        case DFNT_UINT32:
        case DFNT_FLOAT32:
            return 4;
        case DFNT_INT64:
        case DFNT_UINT64:
        case DFNT_FLOAT64:
            return 8;
        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Unknown HDF datatype %d", type)));
            return -1;
    }
}

static inline double 
hdf_value(int32_t type, void *buffer, size_t offset)
{
    switch(type)
    {
        case DFNT_CHAR8:
            return ((signed char *)   buffer)[offset];
        case DFNT_UCHAR8:
            return ((unsigned char *) buffer)[offset];
        case DFNT_INT8:
            return ((int8_t *)        buffer)[offset];
        case DFNT_UINT8:
            return ((uint8_t *)       buffer)[offset];
        case DFNT_INT16:
            return ((int16_t *)       buffer)[offset];
        case DFNT_UINT16:
            return ((uint16_t *)      buffer)[offset];
        case DFNT_INT32:
            return ((int32_t *)       buffer)[offset];
        case DFNT_UINT32:
            return ((uint32_t *)      buffer)[offset];
        case DFNT_INT64:
            return ((int64_t *)       buffer)[offset];
        case DFNT_UINT64:
            return ((uint64_t *)      buffer)[offset];
        case DFNT_FLOAT32:
            return ((float *)         buffer)[offset];
        case DFNT_FLOAT64:
            return ((double *)        buffer)[offset];
        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Unknown HDF datatype %d", type)));
            return -1;
    }   
}

static inline bool
hdf_cmp(int32_t type, void *buffer, size_t offset, void *val)
{
    switch(type)
    {
        case DFNT_CHAR8:
            return ((signed char *)  buffer)[offset] == *((signed char *)  val);
        case DFNT_UCHAR8:
            return ((unsigned char *)buffer)[offset] == *((unsigned char *)val);
        case DFNT_INT8:
            return ((int8_t *)       buffer)[offset] == *((int8_t *)       val);
        case DFNT_UINT8:
            return ((uint8_t *)      buffer)[offset] == *((uint8_t *)      val);
        case DFNT_INT16:
            return ((int16_t *)      buffer)[offset] == *((int16_t *)      val);
        case DFNT_UINT16:
            return ((uint16_t *)     buffer)[offset] == *((uint16_t *)     val);
        case DFNT_INT32:
            return ((int32_t *)      buffer)[offset] == *((int32_t *)      val);
        case DFNT_UINT32:
            return ((uint32_t *)     buffer)[offset] == *((uint32_t *)     val);
        case DFNT_INT64:
            return ((int64_t *)      buffer)[offset] == *((int64_t *)      val);
        case DFNT_UINT64:
            return ((uint64_t *)     buffer)[offset] == *((uint64_t *)     val);
        case DFNT_FLOAT32:
            return ((float *)        buffer)[offset] == *((float *)        val);
        case DFNT_FLOAT64:
            return ((double *)       buffer)[offset] == *((double *)       val);
        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("Unknown HDF datatype %d", type)));
            return -1;
    }   
}

#endif /* _HDF_H_ */
