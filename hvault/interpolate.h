#ifndef _INTERPOLATE_H_
#define _INTERPOLATE_H_

#include <stdlib.h>

void interpolate_line(size_t m, float const *p, float const *n, float *r);
void extrapolate_line(size_t m, float const *p, float const *n, float *r);

#endif
