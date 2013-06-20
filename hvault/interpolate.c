#include "interpolate.h"

/*
 * p1---p2
 * |     |
 * |  o  |
 * |     |
 * p3---p4
 */
static inline float 
interpolate_point(float p1, float p2, float p3, float p4)
{
    return ((float) 0.25) * (p1 + p2 + p3 + p4);
}


/*
 * p1----n1----?
 * |     |     |
 * |     |  o  |
 * |     |     |
 * p2----n2----?
 */
static inline float
extrapolate_point(float n1, float n2, float p1, float p2)
{
    return (3./4.) * (n1 + n2) - (1./4.) * (p1 + p2);
}

/*
 * lu----u-----?
 * |     |     |
 * |     |     |
 * |     |     |
 * l-----c-----?
 * |     |     |
 * |     |  o  |
 * |     |     |
 * ?-----?-----?
 */
static inline float 
extrapolate_corner_point(float c, float l, float u, float lu)
{
    return (9./4.) * c - (3./4.) * (l + u) + (1./4.) * lu;
}

/*
 * ?---p0---p1---p2---p3---     ---pm'---?
 * |    |    |    |    |    ...     |    | 
 * | r0 | r1 | r2 | r3 |    ...  rm'| rm | 
 * |    |    |    |    |    ...     |    | 
 * ?---n0---n1---n2---n3---     ---nm'---?
 */
void
interpolate_line(size_t m, float const *p, float const *n, float *r)
{
    size_t i;
    r[0] = extrapolate_point(p[0], n[0], p[1], n[1]);
    r[m] = extrapolate_point(p[m-1], n[m-1], p[m-2], n[m-2]);
    for (i = 0; i < m-1; i++)
    {
        r[i+1] = interpolate_point(p[i], n[i], p[i+1], n[i+1]);
    }
}

/*
 * ?---p0---p1---p2---p3---     ---pm'---?
 * |    |    |    |    |    ...     |    | 
 * |    |    |    |    |    ...     |    | 
 * |    |    |    |    |    ...     |    | 
 * ?---n0---n1---n2---n3---     ---nm'---?
 * |    |    |    |    |    ...     |    | 
 * | r0 | r1 | r2 | r3 |    ...  rm'| rm | 
 * |    |    |    |    |    ...     |    | 
 * ?----?----?----?----?---     ----?----?
 */
void
extrapolate_line(size_t m, float const *p, float const *n, float *r)
{
    size_t i;
    r[0] = extrapolate_corner_point(n[0], p[0], n[1], p[1]);
    r[m] = extrapolate_corner_point(n[m-1], p[m-1], n[m-2], p[m-2]);
    for (i = 0; i < m-1; i++)
    {
        r[i+1] = extrapolate_point(n[i], n[i+1], p[i], p[i+1]);
    }   
}

void 
hvaultInterpolateFootprint1xOld (float const * restrict src, 
                                 float * restrict dst,
                                 size_t src_h, size_t src_w)
{
    size_t i;
    extrapolate_line(src_w, src + src_w, src, dst);
    for (i = 0; i < src_h - 1; i++)
    {
        interpolate_line(src_w, src + src_w * i, src + src_w * (i + 1), 
                         dst + (src_w + 1) * (i + 1));
    }
    extrapolate_line(src_w, src + src_w * (src_h - 2), 
                            src + src_w * (src_h - 1), 
                            dst + (src_w + 1) * src_h );
}                                 


/*
 * Calculates coefficients of bilinear interpolator.
 *
 * if
 * f(x1, y1) = v[0] = ul
 * f(x1, y2) = v[1] = ur
 * f(x2, y2) = v[2] = lr
 * f(x2, y1) = v[3] = ll
 *
 * then
 * f(x,y) = Sum{i=0..3}( v[i] * res[i] )
 */
static inline void
prepare_point_interpolator (float x1, float x2, float y1, float y2,
                      float x, float y,
                      float * restrict res)
{
    size_t j;
    float c = 1. / ((x2 - x1) * (y2 - y1));

    res[0] = (x2 - x) * (y2 - y);
    res[1] = (x2 - x) * (y - y1);
    res[2] = (x - x1) * (y - y1);
    res[3] = (x - x1) * (y2 - y);

    for (j = 0; j < 4; j++)
    {
        res[j] *= c;
    }
}

static void
prepare_kernel (size_t const src_size, 
                size_t const dst_size, 
                float const x0,
                float const y0,
                float * const kernel)
{
    float x1, x2, y1, y2;
    float c;
    size_t i, j;


    x1 = y1 = 0.5 * (float) src_size;
    x2 = y2 = 1.5 * (float) src_size;

    /* This is manually inlined prepare_point_interpolator calls with 
       x = i, y = j */

    for (i = 0; i < dst_size; i++)
    {
        for (j = 0; j < dst_size; j++)
        {
            float x = (float) i + x0;
            float y = (float) j + y0;
            float * res = kernel + (i * dst_size + j) * 4;

            res[0] = (x2 - x) * (y2 - y);
            res[1] = (x2 - x) * (y - y1);
            res[2] = (x - x1) * (y - y1);
            res[3] = (x - x1) * (y2 - y);
        }
    }

    c = 1. / (float)( src_size * src_size );
    for (i = 0; i < dst_size * dst_size * 4; i++)
    {
        kernel[i] *= c;
    }
}

/*
 * Prepares (2k+1) x (2k+1) x 4 kernel for footprint interpolation, 
 * i.e. corners of each observation
 */
void
hvaultInterpolateFootprintKernel (size_t size, float * kernel)
{
    prepare_kernel(size, 2*size+1, 0.0, 0.0, kernel);
}

/* 
 * Prepares 2k x 2k x 4 kernel for point interpolation,
 * i.e. centers of each observation
 */
void
hvaultInterpolatePointKernel (size_t size, float *kernel)
{
    prepare_kernel(size, 2*size, 0.5, 0.5, kernel);
}

/* 
 * Calculates one box of interpolation with specified kernel.
 * for i = 0..h, j = 0..w
 * res(i, j) = kernel(i, j) * (ul, ur, lr, ll) - scalar product
 * 
 * kernel must be 3d matrix of (h x w x 4) elements
 * kernel_stride is a stride of first dimension, so
 * kernel(i, j, k) = kernel[(i * kernel_stride + j) * 4 + k]
 */
static inline void
calculate_box (float const ul, float const ur, float const lr, float const ll,
               size_t const h, size_t const w,
               float const * const restrict kernel, size_t const kernel_stride,
               float * restrict const res, size_t const res_stride )
{
    size_t i, j;
    for (i = 0; i < h; i++)
    {
        for (j = 0; j < w; j++)
        {
            res[i * res_stride + j] = ul * kernel[4*(i*kernel_stride + j) + 0] 
                                    + ur * kernel[4*(i*kernel_stride + j) + 1]
                                    + lr * kernel[4*(i*kernel_stride + j) + 2]
                                    + ll * kernel[4*(i*kernel_stride + j) + 3];
        }
    }
}

static inline void
calculate_box_straight (float const * const restrict src, 
                        size_t const src_stride,
                        size_t const h, size_t const w,
                        float const * const restrict kernel, 
                        size_t const kernel_stride,
                        float * const restrict res, size_t const res_stride )
{
    size_t i, j;
    for (i = 0; i < h; i++)
    {
        for (j = 0; j < w; j++)
        {
            res[i * res_stride + j] = 
                src[0]            * kernel[4*(i*kernel_stride + j) + 0] +
                src[1]            * kernel[4*(i*kernel_stride + j) + 1] +
                src[src_stride+1] * kernel[4*(i*kernel_stride + j) + 2] +
                src[src_stride]   * kernel[4*(i*kernel_stride + j) + 3];
        }
    }
}

#define src_idx(p, q) *(src+(p)*src_w+(q)), *(src+(p)*src_w+(q)+1), \
                      *(src+((p)+1)*src_w+(q)+1), *(src+((p)+1)*src_w+(q))
#define src_sub(p, q) src + (p)*src_w + (q), src_w
#define krn_sub(p, q) kernel + 4 * ((p) * kstride + (q)), kstride
#define dst_sub(p, q) dst + (p) * rstride + (q), rstride


static inline void
__attribute__((always_inline))
interpolate_grid (float const * const restrict src, float * const restrict dst,
                  size_t const src_h, size_t const src_w,
                  float const * const restrict kernel, size_t const k,
                  size_t const kstride, size_t const rstride, 
                  size_t const small_half, size_t const big_half)
{
    size_t i, j;
    
    /* Upper-left corner */
    calculate_box_straight(
                src_sub(0, 0), 
                small_half, small_half, 
                krn_sub(0, 0),
                dst_sub(0, 0));

    /* Upper border */
    for (j = 0; j < src_w - 1; j++)
    {
        calculate_box_straight(
                src_sub(0, j),
                small_half, k, 
                krn_sub(0, small_half),
                dst_sub(0, small_half + k*j));
    }
    
    /* Upper-right corner */
    calculate_box_straight(
                src_sub(0, src_w - 2),
                small_half, big_half, 
                krn_sub(0, small_half + k),
                dst_sub(0, rstride - big_half));

    for (i = 0; i < src_h - 1; i++)
    {
        /* Left border */
        calculate_box_straight(
                src_sub(i, 0),
                k, small_half,
                krn_sub(small_half, 0),
                dst_sub(small_half + k*i, 0));
        
        /* Middle */
        for (j = 0; j < src_w - 1; j++)
        {
            calculate_box_straight(
                src_sub(i, j),
                k, k,
                krn_sub(small_half, small_half),
                dst_sub(small_half + k*i, small_half + k*j));
        }
        
        /* Right border */
        calculate_box_straight(
                src_sub(i, src_w - 2),
                k, big_half,
                krn_sub(small_half, small_half + k),
                dst_sub(small_half + k*i, rstride - big_half));
    }

    /* Lower-left corner */
    calculate_box_straight(
                src_sub(src_h - 2, 0),
                big_half, small_half,
                krn_sub(small_half + k, 0),
                dst_sub(small_half + k*(src_h-1), 0));

    /* Lower border */
    for (j = 0; j < src_w - 1; j++)
    {
        calculate_box_straight(
                src_sub(src_h - 2, j),
                big_half, k,
                krn_sub(small_half + k, small_half),
                dst_sub(small_half + k*(src_h-1), small_half + k*j));
    }

    /* Lower right corner */
    calculate_box_straight(
                src_sub(src_h - 2, src_w - 2),
                big_half, big_half,
                krn_sub(small_half + k, small_half + k),
                dst_sub(small_half + k*(src_h-1), rstride - big_half));
}

static inline void 
interpolate_footprint (float const * const restrict src, 
                       float * const restrict dst,
                       size_t const src_h, size_t const src_w,
                       float const * const restrict kernel, size_t const k)
{
    size_t const kstride = 2 * k + 1;
    size_t const rstride = k * src_w + 1;

    size_t const small_half = k/2 + k%2;
    size_t const big_half = k/2 + 1;

    interpolate_grid(src, dst, src_h, src_w, kernel, k, 
                     kstride, rstride, small_half, big_half);
}

static inline void
interpolate_points (float const * const restrict src, 
                    float * const restrict dst,
                    size_t const src_h, size_t const src_w, 
                    float const * const restrict kernel, size_t const k)
{
    size_t const kstride = 2 * k;
    size_t const rstride = k * src_w;

    size_t const small_half = k/2;
    size_t const big_half = k/2 + k%2;

    interpolate_grid(src, dst, src_h, src_w, kernel, k, 
                     kstride, rstride, small_half, big_half);
}

#undef src_idx
#undef src_sub
#undef krn_sub
#undef dst_sub

void
hvaultInterpolateFootprint (float const * restrict src,
                            float * restrict dst,
                            size_t src_h, size_t src_w,
                            float const * restrict kernel, size_t k)
{
    interpolate_footprint(src, dst, src_h, src_w, kernel, k);
}

void
hvaultInterpolatePoints (float const * restrict src,
                         float * restrict dst,
                         size_t src_h, size_t src_w,
                         float const * restrict kernel, size_t k)
{
    interpolate_points(src, dst, src_h, src_w, kernel, k);
}


void 
hvaultInterpolateFootprint1x (float const * restrict src, 
                              float * restrict dst,
                              size_t src_h, size_t src_w)
{
    static float const kernel[3*3*4] = {
        +2.25, -0.75, +0.25, -0.75, 
        +0.75, +0.75, -0.25, -0.25, 
        -0.75, +2.25, -0.75, +0.25, 

        +0.75, -0.25, -0.25, +0.75, 
        +0.25, +0.25, +0.25, +0.25, 
        -0.25, +0.75, +0.75, -0.25, 

        -0.75, +0.25, -0.75, +2.25, 
        -0.25, -0.25, +0.75, +0.75, 
        +0.25, -0.75, +2.25, -0.75,
    };
    interpolate_footprint(src, dst, src_h, src_w, kernel, 1);
}

void
hvaultInterpolateFootprint2x (float const * restrict src, 
                              float * restrict dst,
                              size_t src_h, size_t src_w)
{
    static float const kernel[5*5*4] = {
        +2.25, -0.75, +0.25, -0.75, 
        +1.50, +0.00, -0.00, -0.50, 
        +0.75, +0.75, -0.25, -0.25, 
        +0.00, +1.50, -0.50, -0.00, 
        -0.75, +2.25, -0.75, +0.25, 

        +1.50, -0.50, -0.00, +0.00, 
        +1.00, +0.00, +0.00, +0.00, 
        +0.50, +0.50, +0.00, +0.00, 
        +0.00, +1.00, +0.00, +0.00, 
        -0.50, +1.50, +0.00, -0.00, 

        +0.75, -0.25, -0.25, +0.75, 
        +0.50, +0.00, +0.00, +0.50, 
        +0.25, +0.25, +0.25, +0.25, 
        +0.00, +0.50, +0.50, +0.00, 
        -0.25, +0.75, +0.75, -0.25, 

        +0.00, -0.00, -0.50, +1.50, 
        +0.00, +0.00, +0.00, +1.00, 
        +0.00, +0.00, +0.50, +0.50, 
        +0.00, +0.00, +1.00, +0.00, 
        -0.00, +0.00, +1.50, -0.50, 

        -0.75, +0.25, -0.75, +2.25, 
        -0.50, -0.00, +0.00, +1.50, 
        -0.25, -0.25, +0.75, +0.75, 
        -0.00, -0.50, +1.50, +0.00, 
        +0.25, -0.75, +2.25, -0.75,
    };
    interpolate_footprint(src, dst, src_h, src_w, kernel, 2);
}

void
hvaultInterpolateFootprint4x (float const * restrict src, 
                              float * restrict dst,
                              size_t src_h, size_t src_w)
{
    static float const kernel[9*9*4] = {
        +2.2500, -0.7500, +0.2500, -0.7500, 
        +1.8750, -0.3750, +0.1250, -0.6250, 
        +1.5000, +0.0000, -0.0000, -0.5000, 
        +1.1250, +0.3750, -0.1250, -0.3750, 
        +0.7500, +0.7500, -0.2500, -0.2500, 
        +0.3750, +1.1250, -0.3750, -0.1250, 
        +0.0000, +1.5000, -0.5000, -0.0000, 
        -0.3750, +1.8750, -0.6250, +0.1250, 
        -0.7500, +2.2500, -0.7500, +0.2500, 

        +1.8750, -0.6250, +0.1250, -0.3750, 
        +1.5625, -0.3125, +0.0625, -0.3125, 
        +1.2500, +0.0000, -0.0000, -0.2500, 
        +0.9375, +0.3125, -0.0625, -0.1875, 
        +0.6250, +0.6250, -0.1250, -0.1250, 
        +0.3125, +0.9375, -0.1875, -0.0625, 
        +0.0000, +1.2500, -0.2500, -0.0000, 
        -0.3125, +1.5625, -0.3125, +0.0625, 
        -0.6250, +1.8750, -0.3750, +0.1250, 

        +1.5000, -0.5000, -0.0000, +0.0000, 
        +1.2500, -0.2500, -0.0000, +0.0000, 
        +1.0000, +0.0000, +0.0000, +0.0000, 
        +0.7500, +0.2500, +0.0000, +0.0000, 
        +0.5000, +0.5000, +0.0000, +0.0000, 
        +0.2500, +0.7500, +0.0000, +0.0000, 
        +0.0000, +1.0000, +0.0000, +0.0000, 
        -0.2500, +1.2500, +0.0000, -0.0000, 
        -0.5000, +1.5000, +0.0000, -0.0000, 

        +1.1250, -0.3750, -0.1250, +0.3750, 
        +0.9375, -0.1875, -0.0625, +0.3125, 
        +0.7500, +0.0000, +0.0000, +0.2500, 
        +0.5625, +0.1875, +0.0625, +0.1875, 
        +0.3750, +0.3750, +0.1250, +0.1250, 
        +0.1875, +0.5625, +0.1875, +0.0625, 
        +0.0000, +0.7500, +0.2500, +0.0000, 
        -0.1875, +0.9375, +0.3125, -0.0625, 
        -0.3750, +1.1250, +0.3750, -0.1250, 

        +0.7500, -0.2500, -0.2500, +0.7500, 
        +0.6250, -0.1250, -0.1250, +0.6250, 
        +0.5000, +0.0000, +0.0000, +0.5000, 
        +0.3750, +0.1250, +0.1250, +0.3750, 
        +0.2500, +0.2500, +0.2500, +0.2500, 
        +0.1250, +0.3750, +0.3750, +0.1250, 
        +0.0000, +0.5000, +0.5000, +0.0000, 
        -0.1250, +0.6250, +0.6250, -0.1250, 
        -0.2500, +0.7500, +0.7500, -0.2500, 

        +0.3750, -0.1250, -0.3750, +1.1250, 
        +0.3125, -0.0625, -0.1875, +0.9375, 
        +0.2500, +0.0000, +0.0000, +0.7500, 
        +0.1875, +0.0625, +0.1875, +0.5625, 
        +0.1250, +0.1250, +0.3750, +0.3750, 
        +0.0625, +0.1875, +0.5625, +0.1875, 
        +0.0000, +0.2500, +0.7500, +0.0000, 
        -0.0625, +0.3125, +0.9375, -0.1875, 
        -0.1250, +0.3750, +1.1250, -0.3750, 

        +0.0000, -0.0000, -0.5000, +1.5000, 
        +0.0000, -0.0000, -0.2500, +1.2500, 
        +0.0000, +0.0000, +0.0000, +1.0000, 
        +0.0000, +0.0000, +0.2500, +0.7500, 
        +0.0000, +0.0000, +0.5000, +0.5000, 
        +0.0000, +0.0000, +0.7500, +0.2500, 
        +0.0000, +0.0000, +1.0000, +0.0000, 
        -0.0000, +0.0000, +1.2500, -0.2500, 
        -0.0000, +0.0000, +1.5000, -0.5000, 

        -0.3750, +0.1250, -0.6250, +1.8750, 
        -0.3125, +0.0625, -0.3125, +1.5625, 
        -0.2500, -0.0000, +0.0000, +1.2500, 
        -0.1875, -0.0625, +0.3125, +0.9375, 
        -0.1250, -0.1250, +0.6250, +0.6250, 
        -0.0625, -0.1875, +0.9375, +0.3125, 
        -0.0000, -0.2500, +1.2500, +0.0000, 
        +0.0625, -0.3125, +1.5625, -0.3125, 
        +0.1250, -0.3750, +1.8750, -0.6250, 

        -0.7500, +0.2500, -0.7500, +2.2500, 
        -0.6250, +0.1250, -0.3750, +1.8750, 
        -0.5000, -0.0000, +0.0000, +1.5000, 
        -0.3750, -0.1250, +0.3750, +1.1250, 
        -0.2500, -0.2500, +0.7500, +0.7500, 
        -0.1250, -0.3750, +1.1250, +0.3750, 
        -0.0000, -0.5000, +1.5000, +0.0000, 
        +0.1250, -0.6250, +1.8750, -0.3750, 
        +0.2500, -0.7500, +2.2500, -0.7500,
    };
    interpolate_footprint(src, dst, src_h, src_w, kernel, 4);
}



void
hvaultInterpolatePoints2x (float const * restrict src, 
                           float * restrict dst,
                           size_t src_h, size_t src_w)
{
    static float const kernel[4*4*4] = {
        +1.5625, -0.3125, +0.0625, -0.3125, 
        +0.9375, +0.3125, -0.0625, -0.1875, 
        +0.3125, +0.9375, -0.1875, -0.0625, 
        -0.3125, +1.5625, -0.3125, +0.0625, 

        +0.9375, -0.1875, -0.0625, +0.3125, 
        +0.5625, +0.1875, +0.0625, +0.1875, 
        +0.1875, +0.5625, +0.1875, +0.0625, 
        -0.1875, +0.9375, +0.3125, -0.0625, 

        +0.3125, -0.0625, -0.1875, +0.9375, 
        +0.1875, +0.0625, +0.1875, +0.5625, 
        +0.0625, +0.1875, +0.5625, +0.1875, 
        -0.0625, +0.3125, +0.9375, -0.1875, 

        -0.3125, +0.0625, -0.3125, +1.5625, 
        -0.1875, -0.0625, +0.3125, +0.9375, 
        -0.0625, -0.1875, +0.9375, +0.3125, 
        +0.0625, -0.3125, +1.5625, -0.3125,     
    };
    interpolate_points(src, dst, src_h, src_w, kernel, 2);
}

void
hvaultInterpolatePoints4x (float const * restrict src, 
                           float * restrict dst,
                           size_t src_h, size_t src_w)
{
    static float const kernel[8*8*4] = {
        +1.890625, -0.515625, +0.140625, -0.515625, 
        +1.546875, -0.171875, +0.046875, -0.421875, 
        +1.203125, +0.171875, -0.046875, -0.328125, 
        +0.859375, +0.515625, -0.140625, -0.234375, 
        +0.515625, +0.859375, -0.234375, -0.140625, 
        +0.171875, +1.203125, -0.328125, -0.046875, 
        -0.171875, +1.546875, -0.421875, +0.046875, 
        -0.515625, +1.890625, -0.515625, +0.140625, 

        +1.546875, -0.421875, +0.046875, -0.171875, 
        +1.265625, -0.140625, +0.015625, -0.140625, 
        +0.984375, +0.140625, -0.015625, -0.109375, 
        +0.703125, +0.421875, -0.046875, -0.078125, 
        +0.421875, +0.703125, -0.078125, -0.046875, 
        +0.140625, +0.984375, -0.109375, -0.015625, 
        -0.140625, +1.265625, -0.140625, +0.015625, 
        -0.421875, +1.546875, -0.171875, +0.046875, 

        +1.203125, -0.328125, -0.046875, +0.171875, 
        +0.984375, -0.109375, -0.015625, +0.140625, 
        +0.765625, +0.109375, +0.015625, +0.109375, 
        +0.546875, +0.328125, +0.046875, +0.078125, 
        +0.328125, +0.546875, +0.078125, +0.046875, 
        +0.109375, +0.765625, +0.109375, +0.015625, 
        -0.109375, +0.984375, +0.140625, -0.015625, 
        -0.328125, +1.203125, +0.171875, -0.046875, 

        +0.859375, -0.234375, -0.140625, +0.515625, 
        +0.703125, -0.078125, -0.046875, +0.421875, 
        +0.546875, +0.078125, +0.046875, +0.328125, 
        +0.390625, +0.234375, +0.140625, +0.234375, 
        +0.234375, +0.390625, +0.234375, +0.140625, 
        +0.078125, +0.546875, +0.328125, +0.046875, 
        -0.078125, +0.703125, +0.421875, -0.046875, 
        -0.234375, +0.859375, +0.515625, -0.140625, 

        +0.515625, -0.140625, -0.234375, +0.859375, 
        +0.421875, -0.046875, -0.078125, +0.703125, 
        +0.328125, +0.046875, +0.078125, +0.546875, 
        +0.234375, +0.140625, +0.234375, +0.390625, 
        +0.140625, +0.234375, +0.390625, +0.234375, 
        +0.046875, +0.328125, +0.546875, +0.078125, 
        -0.046875, +0.421875, +0.703125, -0.078125, 
        -0.140625, +0.515625, +0.859375, -0.234375, 

        +0.171875, -0.046875, -0.328125, +1.203125, 
        +0.140625, -0.015625, -0.109375, +0.984375, 
        +0.109375, +0.015625, +0.109375, +0.765625, 
        +0.078125, +0.046875, +0.328125, +0.546875, 
        +0.046875, +0.078125, +0.546875, +0.328125, 
        +0.015625, +0.109375, +0.765625, +0.109375, 
        -0.015625, +0.140625, +0.984375, -0.109375, 
        -0.046875, +0.171875, +1.203125, -0.328125, 

        -0.171875, +0.046875, -0.421875, +1.546875, 
        -0.140625, +0.015625, -0.140625, +1.265625, 
        -0.109375, -0.015625, +0.140625, +0.984375, 
        -0.078125, -0.046875, +0.421875, +0.703125, 
        -0.046875, -0.078125, +0.703125, +0.421875, 
        -0.015625, -0.109375, +0.984375, +0.140625, 
        +0.015625, -0.140625, +1.265625, -0.140625, 
        +0.046875, -0.171875, +1.546875, -0.421875, 

        -0.515625, +0.140625, -0.515625, +1.890625, 
        -0.421875, +0.046875, -0.171875, +1.546875, 
        -0.328125, -0.046875, +0.171875, +1.203125, 
        -0.234375, -0.140625, +0.515625, +0.859375, 
        -0.140625, -0.234375, +0.859375, +0.515625, 
        -0.046875, -0.328125, +1.203125, +0.171875, 
        +0.046875, -0.421875, +1.546875, -0.171875, 
        +0.140625, -0.515625, +1.890625, -0.515625,    
    };
    interpolate_points(src, dst, src_h, src_w, kernel, 4);
}
