#!/usr/bin/python

import glob
import os
import re
import psycopg2
from osgeo import gdal

def proc_metadata(mod03):
    gd = gdal.Open(mod03)
    if not gd:
        return (None, None, None)
    
    startdate = gd.GetMetadataItem("RANGEBEGINNINGDATE")
    if not startdate:
        return (None, None, None)
    starttime = gd.GetMetadataItem("RANGEBEGINNINGTIME")
    if not starttime:
        return (None, None, None)
    stopdate  = gd.GetMetadataItem("RANGEENDINGDATE")
    if not stopdate:
        return (None, None, None)
    stoptime  = gd.GetMetadataItem("RANGEENDINGTIME")
    if not stoptime:
        return (None, None, None)

    coord = dict()
    coord["north"] = gd.GetMetadataItem("NORTHBOUNDINGCOORDINATE")
    coord["south"] = gd.GetMetadataItem("SOUTHBOUNDINGCOORDINATE")
    coord["east"] = gd.GetMetadataItem("EASTBOUNDINGCOORDINATE")
    coord["west"] = gd.GetMetadataItem("WESTBOUNDINGCOORDINATE")
    if coord["east"] < 0:
        coord["east"] += 360.0
    if coord["west"] < 0:
        coord["east"] += 360.0

    fp = "POLYGON(( {west} {north}, {east} {north}, {east} {south}, \
                    {west} {south}, {west} {north} ))".format(**coord)
    return (startdate + " " + starttime, stopdate + " " + stoptime, fp)

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

prod_names=['mod03', 'mod021km', 'mod02hkm', 'mod02qkm', 'mod04', 'mod05', \
		    'mod07', 'mod09', 'mod10', 'mod14', 'mod35', 'modhkmds', 'mod1kmds']
# TODO: insert connection parameters here
table_name = "catalog"
connection = psycopg2.connect();
cursor = connection.cursor()
query = "create table " + table_name + \
        "( id        serial    primary key, \
           starttime timestamp not null, \
           stoptime  timestamp not null, \
           footprint geometry  not null, " + \
        " text, ".join(prod_names) + " text );"
cursor.execute(query)
query = "create index on " + table_name + " using gist( footprint )";
cursor.execute(query);

#### Old file layout #####

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


query = "prepare catinsert( timestamp, timestamp, geometry" + \
        ", text" * len(prod_names) + \
        ") as insert into " + table_name + \
        " (starttime, stoptime, footprint, mod03, mod021km, mod02hkm, mod02qkm, " + \
        ", ".join(prod_re.keys()) + ") values (" + \
        ", ".join([ "${0}".format(i) for i in range(1, len(prod_names) + 4) ]) + ");"
print query
cursor.execute(query)

query = "execute catinsert(" + ", ".join(["%s"] * (len(prod_names) + 3)) + ");"
for path in glob.iglob("/mnt/ifs-gis/ftp/*/modis/archive/*/*/?????"):
    dirList = os.listdir(path)

    for mod03 in map(mod03_re.match, dirList):
        if not mod03:
            continue
        time = mod03.group(1)
        mod03_filename = path + "/" + mod03.group(0)
        (starttime, stoptime, fp) = proc_metadata(mod03_filename)
        if not starttime or not stoptime or not fp:
            print "Metadata problem!: ", path, time
            continue
        prod_list = list()
        prod_list.append(starttime)
        prod_list.append(stoptime)
        prod_list.append(fp)
        
        prod_list.append(mod03_filename)
        
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

#### New file layout ####

prod_re["mod03"] = re.compile("^M[OY]D03.*hdf$")
prod_re["mod021km"] = re.compile("^M[OY]D021KM.*hdf$")
prod_re["mod02hkm"] = re.compile("^M[OY]D02HKM.*hdf$")
prod_re["mod02qkm"] = re.compile("^M[OY]D02QKM.*hdf$")

query = "prepare catinsert2( timestamp, timestamp, geometry" + \
        ", text" * len(prod_re) + \
        ") as insert into " + table_name + \
        " (starttime, stoptime, footprint, " + \
        ", ".join(prod_re.keys()) + ") values (" + \
        ", ".join([ "${0}".format(i) for i in range(1, len(prod_names) + 4) ]) + ");"
cursor.execute(query)

query = "execute catinsert2(" + ", ".join(["%s"] * (len(prod_names) + 3)) + ");"
for path in glob.iglob("/mnt/ifs-gis/ftp/*/modis/archive/*/*/?????/????"):
    dirList = os.listdir(path)

    mod03 = filter(prod_re['mod03'].match, dirList)
    if len(mod03) != 1:
        print "Problem with MOD03 in ", path
        continue
    if len(filter(prod_re['mod021km'].match, dirList)) != 1:
        print "Problem with MOD021KM in ", path
        continue

    (starttime, stoptime, fp) = proc_metadata(path + "/" + mod03[0])
    if not starttime or not stoptime or not fp:
        print "Metadata problem!: ", path, time
        continue

    prod_list = list()
    prod_list.append(starttime)
    prod_list.append(stoptime)
    prod_list.append(fp)
    for name in prod_re:
        prod_list.append(list_filter(dirList, prod_re[name], path + "/"))

    cursor.execute(query, tuple(prod_list))
    print path

cursor.execute("delete from " + table_name + " where ST_Ymax(footprint) = 90")

connection.commit()
