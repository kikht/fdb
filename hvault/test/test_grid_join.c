void lwgeom_init_allocators() { lwgeom_install_default_allocators(); }

int main (int argc, char const ** argv) 
{
    LWGEOM * geom; 
    LWPOLY * poly;
    POINT2D orig;
    int res_size;
    int64_t *res_indices;
    double *res_ratio;
    int i;

    
    if( argc < 2 )
        return 0;

    geom = lwgeom_from_wkt( argv[1], LW_PARSER_CHECK_ALL );
    if( geom == NULL ){
        printf( "Error while parsing geom\n" );
        return 1;
    }

    poly = lwgeom_as_lwpoly( geom );
    if( poly == NULL ){
        printf( "Error: is not polygon\n" );
    }
    
    orig.x = 0;
    orig.y = 0;

    grid_join( poly, orig, 0.5, 0.5, &res_size, &res_indices, &res_ratio );
    for( i = 0 ; i < res_size; i++ ){
        int64_t x, y;
        x = res_indices[2*i];
        y = res_indices[2*i+1];

        printf( "%3ld %3ld %lf\n", x, y, res_ratio[i] );
    }

    lwfree( res_indices );
    lwfree( res_ratio );
    lwgeom_free( geom );

    return 0;
}
