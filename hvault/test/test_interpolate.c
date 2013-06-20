#include "../interpolate.h"
#include <stdio.h>
#include <string.h>

#define size 3


void print_array (float * data, size_t h, size_t w)
{
    for (size_t i = 0; i < h; i++) {
        for (size_t j = 0; j < w; j++) {
            printf("% 6.1f, ", data[i*w+j]);
        }
        printf("\n");
    }
    printf("\n");
}

int main() 
{
    float src[size*size] = {   
          0.0, 100.0, 150.0,
        100.0, 200.0, 250.0,
        150.0, 250.0, 300.0};
    float dst[(4*size+1)*(4*size+1)];

    memset(dst, 0xff, sizeof(dst));
    hvaultInterpolateFootprint1x(src, dst, size, size);
    printf("footprint 1x:\n");
    print_array(dst, size+1, size+1);

    memset(dst, 0xff, sizeof(dst));
    hvaultInterpolateFootprint1xOld(src, dst, size, size);
    printf("footprint 1x old:\n");
    print_array(dst, size+1, size+1);

    memset(dst, 0xff, sizeof(dst));
    hvaultInterpolateFootprint2x(src, dst, size, size);
    printf("footprint 2x:\n");
    print_array(dst, 2*size+1, 2*size+1);

    memset(dst, 0xff, sizeof(dst));
    hvaultInterpolateFootprint4x(src, dst, size, size);
    printf("footprint 4x:\n");
    print_array(dst, 4*size+1, 4*size+1);

    memset(dst, 0xff, sizeof(dst));
    hvaultInterpolatePoints2x(src, dst, size, size);
    printf("point 2x:\n");
    print_array(dst, 2*size, 2*size);
    
    memset(dst, 0xff, sizeof(dst));
    hvaultInterpolatePoints4x(src, dst, size, size);
    printf("point 4x:\n");
    print_array(dst, 4*size, 4*size);
}
