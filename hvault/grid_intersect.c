#include "common.h"

static double min (double a, double b) { return a > b ? b : a; }
static double max (double a, double b) { return a > b ? a : b; }

static double
polygon_area (POINT2D * polygon, int size)
{
    int i;
    double area = 0;

    if( size < 3 )
        return 0.0;

    for( i = 0; i < size-1; i++ ) {
        area += polygon[i].x*polygon[i+1].y - polygon[i].y*polygon[i+1].x;
    }
    area += polygon[size-1].x*polygon[0].y - polygon[size-1].y*polygon[0].x;
    return fabs(area) * 0.5;
}

static inline POINT2D grid_point ( POINT2D orig, 
                            double scale_x, 
                            double scale_y, 
                            int i, 
                            int j )
{
    POINT2D res;
    res.x = orig.x + scale_x * ( ( double ) i );
    res.y = orig.y + scale_y * ( ( double ) j );
    return res;
}

static inline POINT2D param_point ( POINT2D *start, POINT2D delta, double p )
{
    POINT2D res;
    res.x = start->x + delta.x * p;
    res.y = start->y + delta.y * p;
    return res;
}

static inline void push_vertex ( POINT2D *res_poly, 
                                 int *res_poly_size, 
                                 int i, 
                                 int j,
                                 int m,
                                 int size,
                                 POINT2D point )
{
    int idx = i * m + j;
    int *psize = res_poly_size + idx;
    POINT2D *poly = res_poly + idx * size * 3 + *psize;
    poly->x = point.x;
    poly->y = point.y;
    (*psize)++;
}

static inline void fswap ( double * a, double * b )
{
    if( *a > *b ){
        double tmp = *a;
        *a = *b;
        *b = tmp;
    }
}

static void 
grid_join_internal ( LWPOLY * polygon, 
                     POINT2D O, 
                     double scale_x, 
                     double scale_y, 
                     int n, 
                     int m, 
                     int64_t imin, 
                     int64_t jmin,
                     int *res_size,
                     int64_t **res_indices,
                     double **res_ratio )
{
    int size;
    double *ph, *pv;
    POINT2D *res_poly;
    int *res_poly_size;
    int line_idx;
    int i, j;
    double *tmp_ratio;
    int idx;
    double cell_area_inv;
    
    Assert( n > 0 );
    Assert( m > 0 );
    Assert( res_size != NULL );
    Assert( res_indices != NULL );
    Assert( res_ratio != NULL );
    Assert( polygon->nrings == 1 );

    ph = lwalloc( sizeof( double ) * ( m + 1 ) );
    pv = lwalloc( sizeof( double ) * ( n + 1 ) );

    size = polygon->rings[0]->npoints - 1;
    res_poly = lwalloc( sizeof( POINT2D ) * m * n * size * 3 );
    res_poly_size = lwalloc( sizeof( POINT2D ) * m * n );
    memset( res_poly_size, 0, sizeof( POINT2D ) * m * n );

    for( line_idx = 0; line_idx < size; line_idx++ ){
        POINT2D * start;
        POINT2D * end;
        POINT2D delta;
        int bx, by;
        
        getPoint2d_p_ro( polygon->rings[0], line_idx, &start );  
        getPoint2d_p_ro( polygon->rings[0], line_idx+1, &end );
        delta.x = end->x - start->x;
        delta.y = end->y - start->y;
        bx = ( end->x > start->x ) ^ ( scale_x < 0 );
        by = ( end->y > start->y ) ^ ( scale_y < 0 );

        /* Prepare parameters */
        if( delta.x != 0 ){
            double inv_delta = 1.0 / delta.x;
            double p1 = scale_x * inv_delta;
            double p2 = ( O.x - start->x ) * inv_delta;
            for( i = 0; i <= n; i++ ){
                pv[i] = p1 * i + p2;
            }
        }
        if( delta.y != 0 ){
            /* ph[i] = (Y(i) - start->y) / delta.y */
            /* Y(i) = O.y + scale_y * j */
            double inv_delta = 1.0 / delta.y;
            double p1 = scale_y * inv_delta;
            double p2 = ( O.y - start->y ) * inv_delta;
            for( i = 0; i <= m; i++ ){
                ph[i] = p1 * i + p2;
            }
        }
        
        if( delta.x == 0 ){
            int line_i = floor( ( start->x - O.x ) / scale_x );
            if( line_i < 0 ){
                for( i = 0; i < n; i++ ){
                    for( j = 0; j < m; j++ ){
                        push_vertex( res_poly, res_poly_size, i, j, m, size, 
                            grid_point( O, scale_x, scale_y, i, j + by ) );
                    }
                }
            } else if( line_i >= n ){
                for( i = 0; i < n; i++ ){
                    for( j = 0; j < m; j++ ){
                        push_vertex( res_poly, res_poly_size, i, j, m, size, 
                            grid_point( O, scale_x, scale_y, i + 1, j + by ) );
                    }
                }
            } else {
                for( i = 0; i < line_i; i++ ){
                    for( j = 0; j < m; j++ ){
                        push_vertex( res_poly, res_poly_size, i, j, m, size, 
                            grid_point( O, scale_x, scale_y, i + 1, j + by ) );
                    }
                }
                
                i = line_i;
                for( j = 0; j < m; j++ ){
                    double b = ph[j+!by];
                    double c = ph[j+by];
                    if( c > 0 && b <= 1 ) {
                        push_vertex( res_poly, res_poly_size, i, j, m, size, 
                            param_point( start, delta, max( 0, b ) ) );
                        push_vertex( res_poly, res_poly_size, i, j, m, size, 
                            param_point( start, delta, min( c, 1 ) ) );
                    }
                }

                for( i = line_i + 1; i < n; i++ ){
                    for( j = 0; j < m; j++ ){
                        push_vertex( res_poly, res_poly_size, i, j, m, size, 
                            grid_point( O, scale_x, scale_y, i, j + by ) );
                    }
                }
            }
        } else if( delta.y == 0 ){
            int line_j = floor( ( start->y - O.y ) / scale_y );
            if( line_j < 0 ){
                for( i = 0; i < n; i++ ){
                    for( j = 0; j < m; j++ ){
                        push_vertex( res_poly, res_poly_size, i, j, m, size, 
                            grid_point( O, scale_x, scale_y, i + bx, j ) );
                    }
                }
            } else if( line_j >= m ){
                for( i = 0; i < n; i++ ){
                    for( j = 0; j < m; j++ ){
                        push_vertex( res_poly, res_poly_size, i, j, m, size, 
                            grid_point( O, scale_x, scale_y, i + bx, j + 1 ) );
                    }
                }
            } else {
                for( i = 0; i < n; i++ ){
                    for( j = 0; j < line_j; j++ ){
                        push_vertex( res_poly, res_poly_size, i, j, m, size, 
                            grid_point( O, scale_x, scale_y, i + bx, j + 1 ) );
                    }
                }

                j = line_j;
                for( i = 0; i < n; i++ ){
                    double b = pv[i+!bx];
                    double c = pv[i+bx];
                    if( c > 0 && b <= 1 ) {
                        push_vertex( res_poly, res_poly_size, i, j, m, size, 
                            param_point( start, delta, max( 0, b ) ) );
                        push_vertex( res_poly, res_poly_size, i, j, m, size, 
                            param_point( start, delta, min( c, 1 ) ) );
                    }
                }

                for( i = 0; i < n; i++ ){
                    for( j = line_j + 1; j < m; j++ ){
                        push_vertex( res_poly, res_poly_size, i, j, m, size, 
                            grid_point( O, scale_x, scale_y, i + bx, j ) );
                    }
                }
            }
        } else {
            for( i = 0; i < n; i++ ){
                for( j = 0; j < m; j++){
                    /* a < b and c < d */
                    double a, b, c, d;
                    a = pv[i+!bx];
                    b = pv[i+bx];
                    c = ph[j+!by];
                    d = ph[j+by];

                    Assert( a < b );
                    Assert( c < d );
                    
                    if( d < a ) {
                        if( d > 0 && d <= 1 ) 
                            push_vertex( res_poly, res_poly_size, i, j, m, size,
                                grid_point( O, scale_x, scale_y, i+!bx, j+by ) );

                        d = b;

                    } else if ( b < c ) {
                        if( b > 0 && b <= 1 )
                            push_vertex( res_poly, res_poly_size, i, j, m, size,
                                grid_point( O, scale_x, scale_y, i+bx, j+!by ) );

                    } else {
                        /* a < d and b < c */
                        double const ta = a, tb = b, tc = c, td = d;

                        /* Sort parameters */ 
                        if( ta < tc ) {
                            a = ta; /* excessive */
                            b = tc;
                        } else {
                            a = tc; /* excessive, not used later */
                            b = ta;
                        }

                        if( tb < td ) {
                            c = tb;
                            d = td; /* excessive */
                        } else {
                            c = td;
                            d = tb; 
                        }

                        Assert(a <= b);
                        Assert(b <= c);
                        Assert(c <= d);
                        
                        if( c > 0 && b <= 1 ) {
                            push_vertex( res_poly, res_poly_size, i, j, m, size,
                                0 <= b ? param_point( start, delta, b ) 
                                       : *start );
                            push_vertex( res_poly, res_poly_size, i, j, m, size,
                                1 >= c ? param_point( start, delta, c )
                                       : *end );
                        }
                    }

                    if( d > 0 && d <= 1 ) 
                        push_vertex( res_poly, res_poly_size, i, j, m, size, 
                            grid_point( O, scale_x, scale_y, i + bx, j + by ) );
                }
            }
        }
    }

    lwfree(ph);
    lwfree(pv);

    tmp_ratio = lwalloc( sizeof(double) * n * m );
    *res_size = 0;
    for( i = 0; i < n * m; i++ ){
        POINT2D * poly = res_poly + i * size * 3;
        tmp_ratio[i] = polygon_area( poly, res_poly_size[i] );

        if( tmp_ratio[i] != 0 ){
            (*res_size)++;
        }
    }

    lwfree(res_poly);
    lwfree(res_poly_size);

    *res_indices = lwalloc( sizeof(int64_t) * *res_size * 2 );
    *res_ratio = lwalloc( sizeof(double) * *res_size );
    idx = 0;
    cell_area_inv = fabs( 1.0 / ( scale_x * scale_y ) );
    for( i = 0; i < n; i++ ){
        for( j = 0; j < m; j++ ){
            if( tmp_ratio[i * m + j] != 0 ){
                (*res_ratio)[idx] = tmp_ratio[i * m + j] * cell_area_inv;
                (*res_indices)[2*idx] = imin + ( ( int64_t ) i ); 
                (*res_indices)[2*idx+1] = jmin + ( ( int64_t ) j );
                idx++;
            }
        }
    }
    lwfree(tmp_ratio);
}


static void 
grid_join ( LWPOLY * polygon, 
            POINT2D orig,
            double scale_x, 
            double scale_y,
            int *res_size,
            int64_t **res_indices,
            double **res_ratio )
{
    GBOX const * bbox;
    double fimin,fimax,fjmin,fjmax;
    int64_t imin,imax,jmin,jmax;
    POINT2D O;
    int n, m;

    bbox = lwgeom_get_bbox( ( LWGEOM * ) polygon ); 

    fimin = ( bbox->xmin - orig.x ) / scale_x;
    fimax = ( bbox->xmax - orig.x ) / scale_x;
    fjmin = ( bbox->ymin - orig.y ) / scale_y;
    fjmax = ( bbox->ymax - orig.y ) / scale_y;

    fswap( &fimin, &fimax );
    fswap( &fjmin, &fjmax );

    imin = floor( fimin );
    imax = ceil ( fimax );
    jmin = floor( fjmin );
    jmax = ceil ( fjmax );
    
    n = imax - imin;
    m = jmax - jmin;
    
    /* Fast path for single cell */
    if( n == 1 && m == 1 ){
        double cell_area_inv = fabs( 1.0 / ( scale_x * scale_y ) );

        *res_size = 1;
        *res_indices = lwalloc( sizeof(int) * 2 );
        *res_ratio = lwalloc( sizeof(double) );
        (*res_indices)[0] = imin;
        (*res_indices)[1] = jmin;
        **res_ratio = lwgeom_area( ( LWGEOM * ) polygon ) * cell_area_inv;
        return;
    }

    O.x = orig.x + imin * scale_x;
    O.y = orig.y + jmin * scale_y;

    grid_join_internal( polygon, O, scale_x, scale_y, n, m, imin, jmin, 
                        res_size, res_indices, res_ratio );
}

static void 
grid_join_area ( LWPOLY * polygon,
                 int width, 
                 int height,
                 double xmin,
                 double ymin,
                 double xmax,
                 double ymax,
                 int *res_size,
                 int64_t **res_indices,
                 double **res_ratio )
{
    GBOX const * bbox;
    double fimin,fimax,fjmin,fjmax;
    int64_t imin,imax,jmin,jmax;
    POINT2D O;
    int n, m;
    double scale_x, scale_y;
    bool overflow = false;

    /* 
     * It is possible that xmin > xmax (or ymin > ymax).
     * It means that we want inverted coordinate along corresponding axis.
     */

    O.x = xmin;
    O.y = ymin;
    
    scale_x = ( xmax - xmin ) / width;
    scale_y = ( ymax - ymin ) / height;

    fswap( &xmin, &xmax );
    fswap( &ymin, &ymax );
    
    bbox = lwgeom_get_bbox( ( LWGEOM * ) polygon );
    /* Fast path for no intersection */
    if( bbox->xmin >= xmax || bbox->ymin >= ymax || 
            bbox->xmax <= xmin || bbox->ymax <= ymin ){
        *res_size = 0;
        *res_indices = NULL;
        *res_ratio = NULL;
        return;
    }

    
    fimin = ( bbox->xmin - O.x ) / scale_x;
    fimax = ( bbox->xmax - O.x ) / scale_x;
    fjmin = ( bbox->ymin - O.y ) / scale_y;
    fjmax = ( bbox->ymax - O.y ) / scale_y;

    fswap( &fimin, &fimax );
    fswap( &fjmin, &fjmax );

    if( fimin < 0 ){
        overflow = true;
        imin = 0;
    } else {
        imin = floor( fimin );
    }

    if( fimax > width ){
        overflow = true;
        imax = width;
    } else {
        imax = ceil( fimax );
    }

    if( fjmin < 0 ){
        overflow = true;
        jmin = 0;
    } else {
        jmin = floor( fjmin );
    }

    if( fjmax > height ){
        overflow = true;
        jmax = height;
    } else {
        jmax = ceil( fjmax );
    }

    n = imax - imin;
    m = jmax - jmin;

    /* Fast path for single cell */
    if( !overflow && n == 1 && m == 1 ){
        double cell_area_inv = fabs( 1.0 / ( scale_x * scale_y ) );

        *res_size = 1;
        *res_indices = lwalloc( sizeof(int) * 2 );
        *res_ratio = lwalloc( sizeof(double) );
        (*res_indices)[0] = imin;
        (*res_indices)[1] = jmin;
        **res_ratio = lwgeom_area( ( LWGEOM * ) polygon ) * cell_area_inv;
        return;
    }

    O.x = O.x + imin * scale_x;
    O.y = O.y + jmin * scale_y;

    grid_join_internal( polygon, O, scale_x, scale_y, n, m, imin, jmin,
                        res_size, res_indices, res_ratio );
}

typedef struct {
    int64_t *indices;
    double *ratio;
} HvaultGridJoinContext;

PG_FUNCTION_INFO_V1(hvault_grid_join);
Datum
hvault_grid_join(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldmemctx;
        GSERIALIZED * gser;
        POINT2D orig;
        double scale_x, scale_y;
        LWGEOM *geom;
        LWPOLY *poly;
        int res_size;
        HvaultGridJoinContext *ctx;

        funcctx = SRF_FIRSTCALL_INIT();
        oldmemctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        if (get_call_result_type(fcinfo, NULL, &funcctx->tuple_desc) 
                != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));
        BlessTupleDesc(funcctx->tuple_desc);

        gser = (GSERIALIZED *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
        scale_x = PG_GETARG_FLOAT8(1);
        scale_y = PG_GETARG_FLOAT8(2);
        orig.x = PG_GETARG_FLOAT8(3);
        orig.y = PG_GETARG_FLOAT8(4);

        geom = lwgeom_from_gserialized(gser);
        if (geom == NULL)
        {
            elog(ERROR, "Can't extract lwgeom from gserialized");
        }

        poly = lwgeom_as_lwpoly(geom);
        if (poly == NULL)
        {
            elog(ERROR, "Geometry is not polygon");
        }
        
        ctx = palloc(sizeof(HvaultGridJoinContext));
        grid_join(poly, orig, scale_x, scale_y, 
                  &res_size, &ctx->indices, &ctx->ratio);
        funcctx->max_calls = res_size;
        funcctx->user_fctx = ctx;
        
        MemoryContextSwitchTo(oldmemctx);
    }
    
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls)
    {
        HvaultGridJoinContext *ctx;
        HeapTuple tuple;
        Datum values[3];
        bool isnull[3] = { false, false, false };
        Datum res;

        ctx = (HvaultGridJoinContext *) funcctx->user_fctx;
        values[0] = Int64GetDatum(ctx->indices[2*funcctx->call_cntr]);
        values[1] = Int64GetDatum(ctx->indices[2*funcctx->call_cntr+1]);
        values[2] = Float8GetDatum(ctx->ratio[funcctx->call_cntr]);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, isnull);
        res = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, res);
    }
    else
    {
        SRF_RETURN_DONE(funcctx);
    }
}

PG_FUNCTION_INFO_V1(hvault_grid_join_area);
Datum
hvault_grid_join_area(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldmemctx;
        GSERIALIZED * gser;
        int width, height;
        double xmin, ymin, xmax, ymax;
        LWGEOM *geom;
        LWPOLY *poly;
        int res_size;
        HvaultGridJoinContext *ctx;

        funcctx = SRF_FIRSTCALL_INIT();
        oldmemctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        if (get_call_result_type(fcinfo, NULL, &funcctx->tuple_desc) 
                != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));
        BlessTupleDesc(funcctx->tuple_desc);
        
        gser = (GSERIALIZED *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
        width = PG_GETARG_INT32(1);
        height = PG_GETARG_INT32(2);
        xmin = PG_GETARG_FLOAT8(3);
        ymin = PG_GETARG_FLOAT8(4);
        xmax = PG_GETARG_FLOAT8(5);
        ymax = PG_GETARG_FLOAT8(6);

        geom = lwgeom_from_gserialized(gser);
        if (geom == NULL)
        {
            elog(ERROR, "Can't extract lwgeom from gserialized");
        }

        poly = lwgeom_as_lwpoly(geom);
        if (poly == NULL)
        {
            elog(ERROR, "Geometry is not polygon");
        }
        
        ctx = palloc(sizeof(HvaultGridJoinContext));
        grid_join_area(poly, width, height, xmin, ymin, xmax, ymax,
                       &res_size, &ctx->indices, &ctx->ratio);
        funcctx->max_calls = res_size;
        funcctx->user_fctx = ctx;
        
        MemoryContextSwitchTo(oldmemctx);
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls)
    {
        HvaultGridJoinContext *ctx;
        HeapTuple tuple;
        Datum values[3];
        bool isnull[3] = { false, false, false };
        Datum res;

        ctx = (HvaultGridJoinContext *) funcctx->user_fctx;
        values[0] = Int32GetDatum(ctx->indices[2*funcctx->call_cntr]);
        values[1] = Int32GetDatum(ctx->indices[2*funcctx->call_cntr+1]);
        values[2] = Float8GetDatum(ctx->ratio[funcctx->call_cntr]);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, isnull);
        res = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, res);
    }
    else
    {
        SRF_RETURN_DONE(funcctx);
    }
}


/*
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
*/
