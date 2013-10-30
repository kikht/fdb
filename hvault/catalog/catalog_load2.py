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
    coord["north"] = float(gd.GetMetadataItem("NORTHBOUNDINGCOORDINATE"))
    coord["south"] = float(gd.GetMetadataItem("SOUTHBOUNDINGCOORDINATE"))
    coord["east"] = float(gd.GetMetadataItem("EASTBOUNDINGCOORDINATE"))
    coord["west"] = float(gd.GetMetadataItem("WESTBOUNDINGCOORDINATE"))
    if coord["east"] < 0:
        coord["east"] += 360.0
    if coord["west"] < 0:
        coord["east"] += 360.0

    fp = "POLYGON(( {west} {north}, {east} {north}, {east} {south}, \
                    {west} {south}, {west} {north} ))".format(**coord)
    return (startdate + " " + starttime, stopdate + " " + stoptime, fp)

def create_catalog(conn, catalog):
    cursor = conn.cursor()
    query = "CREATE TABLE IF NOT EXISTS " + catalog + """ 
        ( id        serial    PRIMARY KEY,
          starttime timestamp NOT NULL,
          stoptime  timestamp NOT NULL,
          footprint geometry  NOT NULL,
          UNIQUE (starttime, stoptime, footprint) )"""
    cursor.execute(query)

def add_catalog_product(conn, catalog, prod):
    cursor = conn.cursor()
    query = """ SELECT 1 FROM information_schema.columns 
            WHERE table_name = %s AND column_name = %s ; """
    cursor.execute(query, (catalog, prod))
    if not cursor.fetchone():
        query = "ALTER TABLE " + catalog + " ADD " + prod + " text ; "
        cursor.execute(query) 

def add_file(conn, catalog, prod, starttime, stoptime, footprint, filename):
    # This is not threadsafe!!! Avoid running multiple copies of script
    cursor = conn.cursor()
    query = "UPDATE " + catalog + " SET " + prod + """ = %s 
        WHERE starttime = %s AND stoptime = %s 
            AND footprint = ST_GeometryFromText(%s) """
    cursor.execute(query, (filename, starttime, stoptime, footprint))
    if (cursor.rowcount == 0):
        query = "INSERT INTO " + catalog + \
            " (starttime, stoptime, footprint, " + prod + \
            ") VALUES (%s, %s, ST_GeometryFromText(%s), %s) "
        cursor.execute(query, (starttime, stoptime, footprint, filename))
    
##########
# MAIN
##########


#TODO: parse commandline
database = 'hvault'
base_path = '/home/kikht/Downloads/hdf'
catalog = 'catalog'
known_products = set(('mod03','mod021km', 'mod02hkm', 'mod02qkm', \
    'mod04_l2', 'mod05_l2', 'mod07_l2', 'mod09', 'mod10_l2', 'mod14', \
    'mod35_l2', 'modhkmds', 'mod1kmds' ))


conn = psycopg2.connect(database=database)
conn.autocommit = True
create_catalog(conn, catalog);
for prod in known_products:
    add_catalog_product(conn, catalog, prod);

prod_re = re.compile("^M[OY]D([^\.]*)\..*\.hdf$")
for curdir, dirnames, filenames in os.walk(base_path):
    for filename in filenames:
        m = prod_re.match(filename)
        if m == None:
            continue
            
        fullname = os.path.join(curdir, filename)
        start, stop, fp = proc_metadata(fullname)
        if start == None or stop == None or fp == None:
            continue
            
        prod = 'mod' + m.group(1).lower()
        if prod not in known_products:
            print "Skipping unknown product " + fullname
            continue
        
        print "Adding file " + fullname
        add_file(conn, catalog, prod, start, stop, fp, fullname)
        
           
conn.close()


