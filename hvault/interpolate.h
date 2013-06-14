#ifndef _INTERPOLATE_H_
#define _INTERPOLATE_H_

#include <stdlib.h>

void interpolate_line(size_t m, float const *p, float const *n, float *r);
void extrapolate_line(size_t m, float const *p, float const *n, float *r);

void hvaultInterpolateFootprintKernel (size_t size, float * kernel);

void hvaultInterpolatePointKernel (size_t size, float *kernel);

void hvaultInterpolateFootprint (float const * src,
                                 float * dst,
                                 size_t src_h, size_t src_w,
                                 float const * kernel, size_t k);

void hvaultInterpolatePoints (float const * src,
                              float * dst,
                              size_t src_h, size_t src_w,
                              float const * kernel, size_t k);


void hvaultInterpolateFootprint1xOld (float const * src, 
                                      float * dst,
                                      size_t src_h, size_t src_w);
void hvaultInterpolateFootprint1x (float const * src, 
                                   float * dst,
                                   size_t src_h, size_t src_w);
void hvaultInterpolateFootprint2x (float const * src, 
                                   float * dst,
                                   size_t src_h, size_t src_w);
void hvaultInterpolateFootprint4x (float const * src, 
                                   float * dst,
                                   size_t src_h, size_t src_w);

void hvaultInterpolatePoints2x (float const * src, 
                                float * dst,
                                size_t src_h, size_t src_w);

void hvaultInterpolatePoints4x (float const * src, 
                                float * dst,
                                size_t src_h, size_t src_w);

#endif
