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

    tile = gd.GetMetadataItem("TileID")
    return {'start':     startdate + " " + starttime,
            'stop':      stopdate + " " + stoptime,
            'footprint': fp,
            'tile':      tile}

def create_catalog(conn, catalog, tile_column):
    cursor = conn.cursor()
    if tile == False:
        query = "CREATE TABLE IF NOT EXISTS " + catalog + """ 
            ( id        serial    PRIMARY KEY,
              starttime timestamp NOT NULL,
              stoptime  timestamp NOT NULL,
              footprint geometry  NOT NULL,
              UNIQUE (starttime, stoptime, footprint) )"""
    else:
        query = "CREATE TABLE IF NOT EXISTS " + catalog + """ 
            ( id        serial    PRIMARY KEY,
              starttime timestamp NOT NULL,
              stoptime  timestamp NOT NULL,
              footprint geometry  NOT NULL,
              tile      text,
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

def add_file(conn, catalog, prod, filename, meta):
    # This is not threadsafe!!! Avoid running multiple copies of script
    cursor = conn.cursor()
    query = "UPDATE " + catalog + " SET " + prod + """ = %s 
        WHERE starttime = %s AND stoptime = %s 
            AND footprint = ST_GeometryFromText(%s) """
    cursor.execute(query, \
                   (filename, meta['start'], meta['stop'], meta['footprint']))
    if (cursor.rowcount == 0):
        if (meta['tile'] == None):
            query = "INSERT INTO " + catalog + \
                " (starttime, stoptime, footprint, " + prod + \
                ") VALUES (%s, %s, ST_GeometryFromText(%s), %s) "
            cursor.execute(query, \
                (filename, meta['start'], meta['stop'], meta['footprint']))
        else:
            query = "INSERT INTO " + catalog + \
                " (starttime, stoptime, footprint, tile, " + prod + \
                ") VALUES (%s, %s, ST_GeometryFromText(%s), %s, %s) "
            cursor.execute(query, (meta['start'], meta['stop'], 
                                   meta['footprint'], meta['tile'], filename))

    
##########
# MAIN
##########


#TODO: parse commandline
database = 'hvault'
base_path = '/home/kikht/Downloads/hdf'
#catalog = 'catalog'
#known_products = set(('mod03','mod021km', 'mod02hkm', 'mod02qkm', \
#    'mod04_l2', 'mod05_l2', 'mod07_l2', 'mod09', 'mod10_l2', 'mod14', \
#    'mod35_l2', 'modhkmds', 'mod1kmds' ))
catalog = 'gdal_catalog'
known_products = set(( 'mod13q1', ))
tile = True # Set to False to disable tile extraction


conn = psycopg2.connect(database=database)
conn.autocommit = True
create_catalog(conn, catalog, tile);
for prod in known_products:
    add_catalog_product(conn, catalog, prod);

prod_re = re.compile("^M[OY]D([^\.]*)\..*\.hdf$")
for curdir, dirnames, filenames in os.walk(base_path):
    for filename in filenames:
        m = prod_re.match(filename)
        if m == None:
            continue
        
        fullname = os.path.join(curdir, filename)
        
        prod = 'mod' + m.group(1).lower()
        if prod not in known_products:
            print "Skipping unknown product " + fullname
            continue
            
        meta = proc_metadata(fullname)
        if meta['start'] == None or meta['stop'] == None \
                or meta['footprint'] == None:
            continue

        if (tile == False):
            meta['tile'] = None
        
        print "Adding file " + fullname
        add_file(conn, catalog, prod, fullname, meta)
        
           
conn.close()


