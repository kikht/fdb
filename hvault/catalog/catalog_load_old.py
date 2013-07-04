#!/usr/bin/python

import glob
import os
import re
import psycopg2
from osgeo import gdal

def proc_metadata(mod03):
    gd = gdal.Open(mod03)
    starttime = gd.GetMetadataItem("RANGEBEGINNINGDATE") + " " + \
                gd.GetMetadataItem("RANGEBEGINNINGTIME")
    stoptime  = gd.GetMetadataItem("RANGEENDINGDATE") + " " + \
                gd.GetMetadataItem("RANGEENDINGTIME")

    coord = dict()
    coord["north"] = gd.GetMetadataItem("NORTHBOUNDINGCOORDINATE")
    coord["south"] = gd.GetMetadataItem("SOUTHBOUNDINGCOORDINATE")
    coord["east"] = gd.GetMetadataItem("EASTBOUNDINGCOORDINATE")
    coord["west"] = gd.GetMetadataItem("WESTBOUNDINGCOORDINATE")
    fp = "POLYGON(( {north} {west}, {north} {east}, {south} {east}, \
                    {south} {west}, {north} {west} ))".format(**coord)
    return (starttime, stoptime, fp)

def list_filter(plist, re, prefix):
    temp_list = filter(re.match, plist)
    if len(temp_list) > 1:
        print "Conflict!: ", temp_list
        temp_list.sort(reverse = True)
    return prefix + temp_list[0] if len(temp_list) > 0 else None

def mod02_filter(plist, re, prefix, time):
    temp_list = [ m.group(0) for m in map(re.match, plist) \
                  if m != None and m.group(1) == time ]
    if len(temp_list) > 1:
        print "Conflict!: ", temp_list
        temp_list.sort(reverse = True)
    return prefix + temp_list[0] if len(temp_list) > 0 else None    



table_name = "catalog3"

prod_re = dict()
mod03_re = re.compile("^M[OY]D03\.A.{7}\.(.{4}).*hdf$")
mod021km_re = re.compile("^M[OY]D021KM\.A.{7}\.(.{4}).*hdf$")
mod02hkm_re = re.compile("^M[OY]D02HKM\.A.{7}\.(.{4}).*hdf$")
mod02qkm_re = re.compile("^M[OY]D02QKM\.A.{7}\.(.{4}).*hdf$")

prod_re["mod04"] = re.compile("^M[OY]D04.*hdf$")
prod_re["mod05"] = re.compile("^M[OY]D05.*hdf$")
prod_re["mod07"] = re.compile("^M[OY]D07.*hdf$")
prod_re["mod09"] = re.compile("^M[OY]D09.*hdf$")
prod_re["mod10"] = re.compile("^M[OY]D10_L2.*hdf$")
prod_re["mod14"] = re.compile("^M[OY]D14.*hdf$")
prod_re["mod35"] = re.compile("^M[OY]D35.*hdf$")
prod_re["modhkmds"] = re.compile("^M[OY]DHKMDS.*hdf$")
prod_re["mod1kmds"] = re.compile("^M[OY]D1KMDS.*hdf$")

# TODO: insert connection parameters here
connection = psycopg2.connect();
query = "prepare catinsert( timestamp, timestamp, geometry, \
         text, text, text, text" + \
        ", text" * len(prod_re) + \
        ") as insert into " + table_name + \
        " (starttime, stoptime, footprint, mod03, mod021km, mod02hkm, mod02qkm" + \
        ", ".join(prod_re.keys()) + ") values (" + \
        ", ".join([ "${0}".format(i) for i in range(1, len(prod_re) + 4) ]) + ");"
cursor.execute(query)

query = "execute catinsert(" + ", ".join(["%s"] * (len(prod_re) + 3)) + ");"
for path in glob.iglob("/mnt/ifs-gis/ftp/terra/modis/archive/2011/*/?????"):
    dirList = os.listdir(path)

    for mod03 in map(mod03_re.match, dirList):
        if not mod03:
            continue
        time = mod03.group(1)
        (starttime, stoptime, fp) = proc_metadata(path + "/" + mod03.group(0))
        prod_list = list()
        prod_list.append(starttime)
        prod_list.append(stoptime)
        prod_list.append(fp)
        
        prod_list.append(mod03.group(0))
        
        prod_list.append(mod02_filter(dirList, mod021km_re, path + "/", time))        
        prod_list.append(mod02_filter(dirList, mod02hkm_re, path + "/", time))        
        prod_list.append(mod02_filter(dirList, mod02qkm_re, path + "/", time))        
        
        subdir = path + "/" + time
        if (os.path.isdir(subdir)):
            subdirList = os.listdir(subdir)
            for name in prod_re:
                prod_list.append(list_filter(subdirList, prod_re[name], subdir + "/"))
        else:
            for name in prod_re:
                prod_list.append(None)

        cursor.execute(query, tuple(prod_list))
        print path, time

connection.commit()
