#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <arpa/inet.h>
#include <geos_c.h>

#define PG_COPY_HEADER_DATA "PGCOPY\n\377\r\n\0\0\0\0\0\0\0\0\0"
#define PG_COPY_HEADER_SIZE 19
#define PG_EPOCH_OFFSET (( int64_t ) 946684800 )


void writeInt16( FILE * out, uint16_t value ) 
{
    uint16_t nval = htons( value );
    fwrite( &nval, 2, 1, out );
}

void writeInt32( FILE * out, uint32_t value ) 
{
    uint32_t nval = htonl( value );
    fwrite( &nval, 4, 1, out );
}

void writeInt64( FILE * out, uint64_t value ) 
{
    uint32_t nval;
    nval = ( uint32_t )( value >> 32 );
    nval = htonl( nval );
    fwrite( &nval, 4, 1, out );
    nval = ( uint32_t )( value & 0xffffffff );
    nval = htonl( nval );
    fwrite( &nval, 4, 1, out );
}

void writeFloat( FILE * out, float value ) 
{
    union {
        float f;
        uint32_t i;
    } swap;
    swap.f = value;
    writeInt32( out, swap.i );
}

void writeDouble( FILE * out, double value ) 
{
    union {
        double d;
        uint64_t i;
    } swap;
    swap.d = value;
    writeInt64( out, swap.i );
}

int main ( int argc, char ** argv )
{
    FILE * output = stdout;
    int32_t intVal = -11;
    char str[] = "test string";
    size_t strLength = strlen( str );
    double doubleVal = 0.1;
    GEOSCoordSequence * coord;
    GEOSGeometry * point;
    GEOSWKBWriter * wkbWriter;
    unsigned char * buf;
    size_t bufSize;
    int64_t timeVal = time( NULL );
    
    /* Write header */
    fwrite( PG_COPY_HEADER_DATA, PG_COPY_HEADER_SIZE, 1, output );
    /* Write tuple size */
    writeInt16( output, 5 );
    
    /* Write int4 */
    writeInt32( output, 4 );
    writeInt32( output, intVal );
    
    /* Write text */
    writeInt32( output, strLength );
    fwrite( str, strLength, 1, output );
    
    /* Write double */
    writeInt32( output, 8 );
    writeDouble( output, doubleVal );
    
    /* Write geometry */
    initGEOS( NULL, NULL );
    coord = GEOSCoordSeq_create( 1, 2 );
    GEOSCoordSeq_setX( coord, 0, 2.0 );
    GEOSCoordSeq_setY( coord, 0, 3.0 );
    point = GEOSGeom_createPoint( coord );
    wkbWriter = GEOSWKBWriter_create();
    buf = GEOSWKBWriter_write( wkbWriter, point, &bufSize );
    writeInt32( output, bufSize );
    fwrite( buf, bufSize, 1, output );
    GEOSFree( buf );
    GEOSWKBWriter_destroy( wkbWriter );
    GEOSGeom_destroy( point );
    finishGEOS();

    /* Write datetime */
    timeVal = ( timeVal - PG_EPOCH_OFFSET ) * 1000000;
    writeInt32( output, 8 );
    writeInt64( output, timeVal );
    
    /*Write Trailer */
    writeInt16( output, -1 );

    return 0;
}
