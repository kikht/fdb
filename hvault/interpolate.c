#include <stdlib.h>

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
