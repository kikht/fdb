#include "../interpolate.h"
#include <stdio.h>

#define k 4
#define stride (2*k)

int main() 
{
    float kernel[stride*stride*4];
    hvaultInterpolatePointKernel(k, kernel);
    for (size_t i = 0; i < stride; i++) {
        for (size_t j = 0; j < stride; j++) {
            for (size_t l = 0; l < 4; l++) {
                printf("%+1.6f, ", kernel[4*(stride*i + j) + l]);
            }
            printf("\n");
        }
        printf("\n");
    }
}
