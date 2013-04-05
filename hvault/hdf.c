#include "hdf.h"

#include <time.h>
#include <utils/memutils.h>

bool
hdf_file_open(HvaultHDFFile *file,
              char const *filename,
              bool has_footprint)
{
    ListCell *l;
    MemoryContext oldmemcxt;

    elog(DEBUG1, "loading hdf file %s", filename);
    file->open_time = clock();
    Assert(file->filememcxt);
    oldmemcxt = MemoryContextSwitchTo(file->filememcxt);

    file->filename = filename;
    file->sd_id = SDstart(file->filename, DFACC_READ);
    if (file->sd_id == FAIL)
    {
        elog(WARNING, "Can't open HDF file %s, skipping file", file->filename);
        return false; 
    }
    
    foreach(l, file->sds)
    {
        HvaultSDSBuffer *sds = (HvaultSDSBuffer *) lfirst(l);
        int32_t sds_idx, rank, dims[H4_MAX_VAR_DIMS], sdnattrs, sdtype;
        double cal_err, offset_err;

        elog(DEBUG1, "Opening SDS %s", sds->name);

        /* Find sds */
        sds_idx = SDnametoindex(file->sd_id, sds->name);
        if (sds_idx == FAIL)
        {
            elog(WARNING, "Can't find dataset %s in file %s, skipping file",
                 sds->name, file->filename);
            MemoryContextSwitchTo(oldmemcxt);
            hdf_file_close(file);
            return false;
        }
        /* Select SDS */
        sds->id = SDselect(file->sd_id, sds_idx);
        if (sds->id == FAIL)
        {
            elog(WARNING, "Can't open dataset %s in file %s, skipping file",
                 sds->name, file->filename);
            MemoryContextSwitchTo(oldmemcxt);
            hdf_file_close(file);
            return false;
        }
        /* Get dimension sizes */
        if (SDgetinfo(sds->id, NULL, &rank, dims, &sds->type, &sdnattrs) == 
            FAIL)
        {
            elog(WARNING, "Can't get info about %s in file %s, skipping file",
                 sds->name, file->filename);
            MemoryContextSwitchTo(oldmemcxt);
            hdf_file_close(file);
            return false;
        }
        if (rank != 2)
        {
            elog(WARNING, "SDS %s in file %s has %dd dataset, skipping file",
                 sds->name, file->filename, rank);
            MemoryContextSwitchTo(oldmemcxt);
            hdf_file_close(file);
            return false;
        }
        if (file->num_lines == -1)
        {
            file->num_lines = dims[0];
            if (file->num_lines < 2 && has_footprint)
            {
                elog(WARNING, 
                     "SDS %s in file %s has %d lines. Can't get footprint, skipping file",
                     sds->name, file->filename, file->num_lines);
                MemoryContextSwitchTo(oldmemcxt);
                hdf_file_close(file);
                return false;
            }
        } 
        else if (dims[0] != file->num_lines)
        {
            elog(WARNING, 
                 "SDS %s in file %s with %d lines is incompatible with others (%d), skipping file",
                 sds->name, file->filename, dims[0], file->num_lines);
            MemoryContextSwitchTo(oldmemcxt);
            hdf_file_close(file);
            return false;
        }
        if (file->num_samples == -1)
        {
            file->num_samples = dims[1];
        } 
        else if (dims[1] != file->num_samples)
        {
            elog(WARNING, 
                 "SDS %s in file %s with %d samples is incompatible with others (%d), skipping file",
                 sds->name, file->filename, dims[1], file->num_samples);
            MemoryContextSwitchTo(oldmemcxt);
            hdf_file_close(file);
            return false;
        }

        //TODO: check SDS datatypes 


        /* Get scale, offset & fill */
        sds->fill_val = palloc(hdf_sizeof(sds->type));
        if (SDgetfillvalue(sds->id, sds->fill_val) != SUCCEED)
        {
            pfree(sds->fill_val);
            sds->fill_val = NULL;
        }
        if (SDgetcal(sds->id, &sds->scale, &cal_err, &sds->offset, 
                     &offset_err, &sdtype) != SUCCEED)
        {
            sds->scale = 1.;
            sds->offset = 0;
        }
    }

    if (file->num_lines < 0 || file->num_samples < 0)
    {
        elog(WARNING, "Can't get number of lines and samples");
        MemoryContextSwitchTo(oldmemcxt);
        hdf_file_close(file);
        return false;
    }
    /* Allocate footprint buffers */
    /* TODO: check that allocation is OK */
    if (has_footprint)
    {
        size_t bufsize = (file->num_samples + 1);
        file->prevbrdlat = (float *) palloc(sizeof(float) * bufsize);
        file->prevbrdlon = (float *) palloc(sizeof(float) * bufsize);
        file->nextbrdlat = (float *) palloc(sizeof(float) * bufsize);
        file->nextbrdlon = (float *) palloc(sizeof(float) * bufsize);
    }
    /* Allocate sd buffers */
    foreach(l, file->sds)
    {
        HvaultSDSBuffer *sds = (HvaultSDSBuffer *) lfirst(l);
        sds->cur = palloc(hdf_sizeof(sds->type) * file->num_samples);
        if (sds->haswindow)
        {
            sds->next = palloc(hdf_sizeof(sds->type) *
                               file->num_samples);
            sds->prev = palloc(hdf_sizeof(sds->type) *
                               file->num_samples);
        }
    }
    /* Allocate predicate selection column */
    file->sel = palloc(sizeof(size_t) * file->num_samples);
    file->sel_size = -1;
    MemoryContextSwitchTo(oldmemcxt);
    return true;
}

void   
hdf_file_close(HvaultHDFFile *file)
{
    ListCell *l;

    Assert(file->sd_id != FAIL);
    foreach(l, file->sds)
    {
        HvaultSDSBuffer *sds = (HvaultSDSBuffer *) lfirst(l);
        if (sds->id == FAIL) continue;
        if (SDendaccess(sds->id) == FAIL)
        {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), 
                            errmsg("Can't close SDS")));
        }
        sds->id = FAIL;
        sds->cur = sds->next = sds->prev = NULL;
        sds->fill_val = NULL;
    }
    if (SDend(file->sd_id) == FAIL)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't close HDF file")));
    }

    file->sd_id = FAIL;
    file->filename = NULL;
    MemoryContextReset(file->filememcxt);
    file->prevbrdlat = file->prevbrdlon = file->nextbrdlat = 
                       file->nextbrdlon = NULL;
    file->num_samples = -1;
    file->num_lines = -1;
    file->sel_size = -1;
    file->sel = NULL;
    elog(DEBUG1, "File processed in %f sec", 
         ((float) (clock() - file->open_time)) / CLOCKS_PER_SEC);
}
