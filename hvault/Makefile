INC=-I/usr/pgsql-9.2/include/server/
HDFLIB=-L/usr/lib/hdf -lmfhdf -ldf -ljpeg -lz 
LIB=-l:/usr/pgsql-9.2/lib/postgis-2.0.so $(HDFLIB)
	
CFLAGS = -std=c11 -D_POSIX_C_SOURCE -g -Wall -pedantic 
# optimization flags
# CFLAGS := $(CFLAGS) -O3 -flto -march=native

hvault: hvault.so
	psql gis-test -c "drop extension hvault; create extension hvault;"

hvault.so: hvault.o interpolate.o hdf.o metaload.o plan.o
	gcc $(CFLAGS) -shared -Wl,-soname,$@ -o $@ $^ $(LIB)

metaload.o: metaload.c
	gcc $(CFLAGS) $(INC) -o $@ -c $<

interpolate.o: interpolate.c
	gcc $(CFLAGS) $(INC) -o $@ -c $<

hdf.o: hdf.c hdf.h hvault.h
	gcc $(CFLAGS) $(INC) -o $@ -c $<	

plan.o: plan.c hvault.h
	gcc $(CFLAGS) $(INC) -o $@ -c $<

hvault.o: hvault.c hvault.h hdf.h
	gcc $(CFLAGS) $(INC) -o $@ -c $<

clean:
	rm -rf *.o *.so
