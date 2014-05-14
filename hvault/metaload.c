#define _FILE_OFFSET_BITS 64
#define _LARGEFILE_SOURCE

#include <fnmatch.h>
#include <ftw.h>
#include <regex.h>
#include <sys/stat.h>
#include <sys/types.h>

/* PostgreSQL */
#include <postgres.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <executor/spi.h>
#include <funcapi.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <lib/stringinfo.h>

/* HDF */
#define int8 hdf_int8
#include <hdf/mfhdf.h>
#include <hdf/hlimits.h>
#undef int8

/* PostGIS */
#include <liblwgeom.h>

extern Datum hvault_load_modis_swath(PG_FUNCTION_ARGS);
extern Datum hvault_mass_load_modis_swath(PG_FUNCTION_ARGS);

#define ATTR_REGEX_TEMPLATE "OBJECT *= %s.*VALUE *= \\(.*\\)\n.*END_OBJECT *= %s"
#define INFO_INSERT_QUERY "INSERT INTO %s \
    (filename, starttime, stoptime, footprint, size) \
    VALUES ($1, $2, $3, $4, $5)"
#define INFO_CHECK_QUERY "SELECT 1 FROM %s WHERE filename = $1"

typedef struct 
{
    char const * filename;
    Datum geom;
    Datum start, stop;
    Datum size;
} ModisSwathInfo;

typedef struct 
{
    regex_t north, south, west, east;
    regex_t startdate, starttime, stopdate, stoptime;
    StringInfo attr_buffer, date_buffer;
} ModisSwathRegex;

static int 
get_attr_regex(regex_t * regex, char const *attr)
{
    int ret;
    char *regex_str;

    regex_str = malloc(sizeof(ATTR_REGEX_TEMPLATE) + 2*strlen(attr) + 1);
    sprintf(regex_str, ATTR_REGEX_TEMPLATE, attr, attr);
    ret = regcomp(regex, regex_str, 0);

    free(regex_str);
    return ret;
}

static int
init_modis_swath_regex(ModisSwathRegex *regex)
{
    int ret;
    ret = get_attr_regex(&regex->north,     "NORTHBOUNDINGCOORDINATE");
    if (ret) return ret;
    ret = get_attr_regex(&regex->south,     "SOUTHBOUNDINGCOORDINATE");
    if (ret) return ret;
    ret = get_attr_regex(&regex->east,      "EASTBOUNDINGCOORDINATE");
    if (ret) return ret;
    ret = get_attr_regex(&regex->west,      "WESTBOUNDINGCOORDINATE");
    if (ret) return ret;
    ret = get_attr_regex(&regex->starttime, "RANGEBEGINNINGTIME");
    if (ret) return ret;
    ret = get_attr_regex(&regex->startdate, "RANGEBEGINNINGDATE");
    if (ret) return ret;
    ret = get_attr_regex(&regex->stoptime,  "RANGEENDINGTIME");
    if (ret) return ret;
    ret = get_attr_regex(&regex->stopdate,  "RANGEENDINGDATE");
    if (ret) return ret;

    regex->attr_buffer = makeStringInfo();
    regex->date_buffer = makeStringInfo();
    return 0;
}

static void
free_modis_swath_regex(ModisSwathRegex *regex)
{
    pfree(regex->attr_buffer->data);
    pfree(regex->attr_buffer);
    pfree(regex->date_buffer->data);
    pfree(regex->date_buffer);
    regfree(&regex->north);
    regfree(&regex->south);
    regfree(&regex->east);
    regfree(&regex->west);
    regfree(&regex->startdate);
    regfree(&regex->starttime);
    regfree(&regex->stopdate);
    regfree(&regex->stoptime);
}

static bool 
match_float(regex_t *regex, char const *data, double *res)
{
    regmatch_t match[2];

    if (regexec(regex, data, 2, match, 0) != 0)
    {
        return false;
    }

    if (match[1].rm_so == -1 || match[1].rm_eo == -1)
    {
        return false;
    }

    errno = 0;
    *res = strtod(data + match[1].rm_so, NULL);
    return errno == 0;
}

static bool
match_datetime(regex_t *re_date, 
               regex_t *re_time, 
               char const *data,
               StringInfo work,
               Datum *result)
{
    regmatch_t match_date[2], match_time[2];

    if (regexec(re_date, data, 2, match_date, 0) != 0 ||
        regexec(re_time, data, 2, match_time, 0) != 0)
    {
        elog(WARNING, "Can't match regex");
        return false;
    }

    if (match_date[1].rm_so == -1 || match_date[1].rm_eo == -1 ||
        match_time[1].rm_so == -1 || match_time[1].rm_so == -1)
    {
        elog(WARNING, "Can't match subexpression");
        return false;
    }

    if (match_date[1].rm_eo - match_date[1].rm_so <= 2 ||
        match_time[1].rm_eo - match_time[1].rm_so <= 2)
    {
        elog(WARNING, "Matched value is too small");
        return false;
    }
    resetStringInfo(work);
    appendBinaryStringInfo(work, data + match_date[1].rm_so + 1, 
                           match_date[1].rm_eo - match_date[1].rm_so - 2);
    appendStringInfoCharMacro(work, ' ');
    appendBinaryStringInfo(work, data + match_time[1].rm_so + 1, 
                           match_time[1].rm_eo - match_time[1].rm_so - 2);
    appendStringInfoCharMacro(work, '\0');
    elog(DEBUG1, "Parsing datetime %s", work->data);

    *result = DirectFunctionCall3(&timestamp_in, CStringGetDatum(work->data),
                                  PointerGetDatum(NULL), Int32GetDatum(-1));
    return true;
}

static bool
get_modis_attr(int32_t sd_id, char const * attr, StringInfo result)
{
    int32_t att_idx, type, size;
    char attr_name[H4_MAX_NC_NAME];

    att_idx = SDfindattr(sd_id, attr);
    if (att_idx == FAIL)
    {
        elog(WARNING, "Can't find attribute %s", attr);
        return false;
    }

    if (SDattrinfo(sd_id, att_idx, attr_name, &type, &size) == FAIL)
    {
        elog(WARNING, "Can't get attribute info for %s", attr);
        return false;
    }

    if (result->len <= size)
    {
        enlargeStringInfo(result, size + 1 - result->len);
    }

    if (SDreadattr(sd_id, att_idx, result->data) == FAIL)
    {
        elog(WARNING, "Can't read attribute %s", attr);
        return false;
    }

    result->data[size] = '\0';
    return true;
}

static bool
get_modis_swath_info(ModisSwathInfo *info, 
                     ModisSwathRegex *regex,
                     bool shift_longitude)
{
    bool retval = false;
    struct stat stats; 
    int32_t sd_id = FAIL;
    double north, south, east, west;
    POINTARRAY *ptarray;
    LWGEOM *geom;
    LWPOLY *poly;
    GSERIALIZED *gserialized;
    POINT4D point = {0, 0, 0, 0};
    size_t size;

    do 
    {
        if (stat(info->filename, &stats))
        {
            elog(WARNING, "Can't get file stats");
            break;
        }
        info->size = Int64GetDatum(stats.st_size);

        sd_id = SDstart(info->filename, DFACC_READ);
        if (sd_id == FAIL)
        {
            elog(WARNING, "Can't open file");
            break;
        }

        if (!get_modis_attr(sd_id, "ArchiveMetadata.0", regex->attr_buffer))
        {
            elog(WARNING, "Can't read archive metadata");
            break;
        }

        if (!match_float(&regex->north, regex->attr_buffer->data, &north))
        {
            elog(WARNING, "Can't read north bounding coordinate");
            break;
        }
        if (!match_float(&regex->south, regex->attr_buffer->data, &south))
        {
            elog(WARNING, "Can't read south bounding coordinate");
            break;
        }
        if (!match_float(&regex->east,  regex->attr_buffer->data, &east))
        {
            elog(WARNING, "Can't read east bounding coordinate");
            break;
        }
        if (!match_float(&regex->west,  regex->attr_buffer->data, &west))
        {
            elog(WARNING, "Can't read west bounding coordinate");
            break;
        }

        if (!get_modis_attr(sd_id, "CoreMetadata.0", regex->attr_buffer))
        {
            elog(WARNING, "Can't read core metadata");
            break;
        }

        if (!match_datetime(&regex->startdate, &regex->starttime, 
                            regex->attr_buffer->data, regex->date_buffer, 
                            &info->start))
        {
            elog(WARNING, "Can't get start time");
            break;
        }
        if (!match_datetime(&regex->stopdate, &regex->stoptime, 
                            regex->attr_buffer->data, regex->date_buffer, 
                            &info->stop))
        {
            elog(WARNING, "Can't get stop time");
            break;
        }

        if (shift_longitude) 
        {
            west += 360 * (west < 0);
            east += 360 * (east < 0);
        }

        ptarray = ptarray_construct(false, false, 5);
        point.y = north;
        point.x = west;
        ptarray_set_point4d(ptarray, 0, &point);
        point.y = north;
        point.x = east;
        ptarray_set_point4d(ptarray, 1, &point);
        point.y = south;
        point.x = east;
        ptarray_set_point4d(ptarray, 2, &point);
        point.y = south;
        point.x = west;
        ptarray_set_point4d(ptarray, 3, &point);
        point.y = north;
        point.x = west;
        ptarray_set_point4d(ptarray, 4, &point);

        poly = lwpoly_construct(SRID_UNKNOWN, NULL, 1, &ptarray);
        geom = lwpoly_as_lwgeom(poly);
        gserialized = gserialized_from_lwgeom(geom, false, &size);
        info->geom = PointerGetDatum(gserialized);

        retval = true;  
    } while(0);
    
    if (sd_id != FAIL)
        SDend(sd_id);

    return retval;
}

static SPIPlanPtr
prepare_insert_plan(char const *catalog)
{
    StringInfo query = makeStringInfo();
    SPIPlanPtr plan;
    Oid argtypes[5];

    argtypes[0] = CSTRINGOID;
    argtypes[1] = TIMESTAMPOID;
    argtypes[2] = TIMESTAMPOID;
    argtypes[3] = TypenameGetTypid("geometry");
    argtypes[4] = INT8OID;

    appendStringInfo(query, INFO_INSERT_QUERY, catalog);
    plan = SPI_prepare(query->data, 5, argtypes);

    pfree(query->data);
    pfree(query);
    return plan;
}

static bool
execute_insert_plan(SPIPlanPtr plan, ModisSwathInfo const *info)
{
    Datum values[5];
    
    values[0] = CStringGetDatum(info->filename);
    values[1] = info->start;
    values[2] = info->stop;
    values[3] = info->geom;
    values[4] = info->size;

    return SPI_execute_plan(plan, values, NULL, false, 1) == SPI_OK_INSERT;
}

static SPIPlanPtr
prepare_check_plan(char const *catalog)
{
    StringInfo query = makeStringInfo();
    SPIPlanPtr plan;
    Oid argtypes[1];

    argtypes[0] = TEXTOID;

    appendStringInfo(query, INFO_CHECK_QUERY, catalog);
    plan = SPI_prepare(query->data, 1, argtypes);

    pfree(query->data);
    pfree(query);
    return plan;   
}

static bool 
check_modis_swath_exists(SPIPlanPtr plan, char const * file)
{
    Datum values[1];
    values[0] = CStringGetTextDatum(file);
    if (SPI_execute_plan(plan, values, NULL, true, 1) == SPI_OK_SELECT)
    {
        return SPI_processed > 0;   
    } 
    else 
    {
        elog(WARNING, "Can't execute check query");
        return true;
    }
}

static bool
store_modis_swath_info(ModisSwathInfo const *info, char const *catalog)
{
    bool retval = true;
    SPIPlanPtr plan;

    if (SPI_connect() != SPI_OK_CONNECT)
        return false;

    plan = prepare_insert_plan(catalog);
    if (plan != NULL)
    {
        retval = execute_insert_plan(plan, info);
        if (SPI_freeplan(plan) != 0)
            retval = false;
        
    }

    if (SPI_finish() != SPI_OK_FINISH)    
        retval = false;
    return retval;
}

static bool
load_modis_swath(char const *catalog, 
                 char const *filename, 
                 bool shift_longitude)
{
    ModisSwathRegex regex;
    ModisSwathInfo info;
    bool retval = false;

    if (init_modis_swath_regex(&regex) != 0)
    {
        elog(ERROR, "Can't initialize regexs");
        return false;
    }

    info.filename = filename;
    if (get_modis_swath_info(&info, &regex, shift_longitude))
    {
        if (store_modis_swath_info(&info, catalog))
        {
            retval = true;
        }
        else
        {
            elog(WARNING, "Can't insert %s info into catalog %s",
                 filename, catalog);
        }
    }
    else
    {
        elog(WARNING, "Can't read swath info from %s", filename);
    }

    free_modis_swath_regex(&regex);
    return retval;
}

PG_FUNCTION_INFO_V1(hvault_load_modis_swath);
Datum 
hvault_load_modis_swath(PG_FUNCTION_ARGS)
{
    char const *catalog = PG_GETARG_CSTRING(0);
    char const *filename = PG_GETARG_CSTRING(1);
    bool shift_longitude = PG_GETARG_BOOL(2);

    PG_RETURN_BOOL(load_modis_swath(catalog, filename, shift_longitude));
}

static struct {
    char const *pattern;
    ModisSwathRegex regex;
    ModisSwathInfo info;
    SPIPlanPtr insert_plan, check_plan;
    int num_files;
    bool shift_longitude;
} mass_load_ctx;

static int
mass_load_modis_swath_walker(char const * fpath,
                             struct stat const *sb,
                             int typeflag)
{
    (void)(sb);
    char const *basename = fpath;
    char const *temp = fpath;
    if (typeflag != FTW_F)
        return 0;

    while (*temp)
    {
        if (*temp == '/')
        {
            basename = temp+1;
        }
        temp++;
    }

    if (fnmatch(mass_load_ctx.pattern, basename, FNM_PATHNAME) == 0)
    {
        elog(NOTICE, "Processing %s", fpath);
        if (!check_modis_swath_exists(mass_load_ctx.check_plan, fpath))
        {
            mass_load_ctx.info.filename = fpath;
            if (get_modis_swath_info(&mass_load_ctx.info, 
                                     &mass_load_ctx.regex,
                                     mass_load_ctx.shift_longitude))
            {
                if (execute_insert_plan(mass_load_ctx.insert_plan, 
                                        &mass_load_ctx.info))
                {
                    mass_load_ctx.num_files++;
                }
                else
                {
                    elog(WARNING, "Can't insert swath info from %s", fpath);
                }
            }
            else
            {
                elog(WARNING, "Can't read swath info from %s", fpath);
            }
        } 
        else 
        {
            elog(DEBUG1, "Skipping %s, already exists", fpath);
        }

    }
    return 0;
}

static int
mass_load_modis_swath(char const *catalog, 
                      char const *path, 
                      char const *pattern,
                      bool shift_longitude)
{
    mass_load_ctx.num_files = 0;
    if (SPI_connect() != SPI_OK_CONNECT)
        return -1;

    mass_load_ctx.shift_longitude = shift_longitude;
    mass_load_ctx.pattern = pattern;
    mass_load_ctx.insert_plan = prepare_insert_plan(catalog);
    if (mass_load_ctx.insert_plan != NULL)
    {
        mass_load_ctx.check_plan = prepare_check_plan(catalog);
        if (mass_load_ctx.check_plan != NULL)
        {
            if (init_modis_swath_regex(&mass_load_ctx.regex) == 0)
            {
                if (ftw(path, &mass_load_modis_swath_walker, 10) != 0)
                {
                    elog(WARNING, "Error while walking directory tree: %s",
                         strerror(errno));
                }
                free_modis_swath_regex(&mass_load_ctx.regex);
            }
            else
            {
                elog(WARNING, "Can't initialize regexs");
                mass_load_ctx.num_files = -1;
            }
            SPI_freeplan(mass_load_ctx.check_plan);
        }
        SPI_freeplan(mass_load_ctx.insert_plan);
    }
    else
    {
        elog(WARNING, "Can't prepare insert plan");
        mass_load_ctx.num_files = -1;
    }
    
    if (SPI_finish() != SPI_OK_FINISH)    
        return -1;
    return mass_load_ctx.num_files;    
}

PG_FUNCTION_INFO_V1(hvault_mass_load_modis_swath);
Datum 
hvault_mass_load_modis_swath(PG_FUNCTION_ARGS)
{
    char const *catalog = PG_GETARG_CSTRING(0);
    char const *dir = PG_GETARG_CSTRING(1);
    char const *pattern = PG_GETARG_CSTRING(2);
    bool shift_longitude = PG_GETARG_BOOL(3);

    PG_RETURN_INT32(mass_load_modis_swath(catalog, dir, pattern, 
                                          shift_longitude));
}
