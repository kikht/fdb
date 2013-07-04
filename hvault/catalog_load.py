#!/usr/bin/python

import glob
import os
import re
import psycopg2
from osgeo import gdal

table_name = "catalog2"

prod_re = dict()
prod_re["mod03"] = re.compile("^M[OY]D03.*hdf$")
prod_re["mod04"] = re.compile("^M[OY]D04.*hdf$")
prod_re["mod05"] = re.compile("^M[OY]D05.*hdf$")
prod_re["mod07"] = re.compile("^M[OY]D07.*hdf$")
prod_re["mod09"] = re.compile("^M[OY]D09.*hdf$")
prod_re["mod10"] = re.compile("^M[OY]D10.*hdf$")
prod_re["mod14"] = re.compile("^M[OY]D14.*hdf$")
prod_re["mod35"] = re.compile("^M[OY]D35.*hdf$")
prod_re["mod021km"] = re.compile("^M[OY]D021KM.*hdf$")
prod_re["mod02hkm"] = re.compile("^M[OY]D02HKM.*hdf$")
prod_re["mod02qkm"] = re.compile("^M[OY]D02QKM.*hdf$")
prod_re["modhkmds"] = re.compile("^M[OY]DHKMDS.*hdf$")
prod_re["mod1kmds"] = re.compile("^M[OY]D1KMDS.*hdf$")

# TODO: insert connection parameters here
connection = psycopg2.connect()
cursor = connection.cursor()
query = "create table " + table_name + 
        "( id        serial    primary key, \
           starttime timestamp not null, \
           stoptime  timestamp not null, \
           footprint geometry  not null, " + 
        " text, ".join(prod_re.keys()) + " );"
cursor.execute(query)
query = "create index on " + table_name + " using gist( footprint )";
cursor.execute(query);

query = "prepare catinsert( timestamp, timestamp, geometry" + 
        ", text" * len(prod_re) + 
        ") as insert into " + table_name + 
        " (starttime, stoptime, footprint, " + 
        ", ".join(prod_re.keys()) + ") values (" + 
        ", ".join([ "${0}".format(i) for i in range(1, len(prod_re) + 4) ]) + ");"
cursor.execute(query)

query = "execute catinsert(" + ", ".join(["%s"] * (len(prod_re) + 3)) + ");"
for path in glob.iglob("/mnt/ifs-gis/ftp/*/modis/archive/*/*/?????/????"):
    dirList = os.listdir(path)

    mod03 = filter(prod_re['mod03'].match, dirList)
    if len(mod03) != 1:
        print "Problem with MOD03 in ", path
        continue

    gd = gdal.Open(path + "/" + mod03[0])
    starttime = gd.GetMetadataItem("RANGEBEGINNINGDATE") + " " + 
                gd.GetMetadataItem("RANGEBEGINNINGTIME")
    stoptime  = gd.GetMetadataItem("RANGEENDINGDATE") + " " + 
                gd.GetMetadataItem("RANGEENDINGTIME")

    coord = dict()
    coord["north"] = gd.GetMetadataItem("NORTHBOUNDINGCOORDINATE")
    coord["south"] = gd.GetMetadataItem("SOUTHBOUNDINGCOORDINATE")
    coord["east"] = gd.GetMetadataItem("EASTBOUNDINGCOORDINATE")
    coord["west"] = gd.GetMetadataItem("WESTBOUNDINGCOORDINATE")
    fp = "POLYGON(( {north} {west}, {north} {east}, {south} {east}, \
                    {south} {west}, {north} {west} ))".format(**coord)

    prod_list = list()
    prod_list.append(starttime)
    prod_list.append(stoptime)
    prod_list.append(fp)
    for name in prod_re:
        temp_list = filter(prod_re[name].match, dirList)
        prod_list.append(path+"/"+temp_list[0] if len(temp_list) > 0 else None)
        if len(temp_list) > 1:
            print "Conflict!: ", temp_list
    cursor.execute(query, tuple(prod_list))
    print path

connection.commit()
