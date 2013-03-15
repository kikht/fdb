POSTGRES_PREFIX=/usr/pgsql-9.2

INC=-I$(POSTGRES_PREFIX)/include/server/
HDFLIB=-L/usr/lib{,64}/hdf -lmfhdf -ldf -ljpeg -lz 
LIB=-l:$(POSTGRES_PREFIX)/lib/postgis-2.0.so $(HDFLIB)
	
CFLAGS = -std=c99 -D_POSIX_C_SOURCE -fPIC -g -Wall -pedantic \
		 -DUSE_ASSERT_CHECKING
# optimization flags
# CFLAGS := $(CFLAGS) -O3 -march=native -UUSE_ASSERT_CHECKING

hvault.so: hvault.o interpolate.o hdf.o metaload.o plan.o utils.o predicates.o
	gcc $(CFLAGS) -shared -Wl,-soname,$@ -o $@ $^ $(LIB)

metaload.o: metaload.c
	gcc $(CFLAGS) $(INC) -o $@ -c $<

utils.o: utils.c hvault.h
	gcc $(CFLAGS) $(INC) -o $@ -c $<

interpolate.o: interpolate.c
	gcc $(CFLAGS) $(INC) -o $@ -c $<

hdf.o: hdf.c hdf.h hvault.h
	gcc $(CFLAGS) $(INC) -o $@ -c $<	

plan.o: plan.c hvault.h
	gcc $(CFLAGS) $(INC) -o $@ -c $<

hvault.o: hvault.c hvault.h hdf.h
	gcc $(CFLAGS) $(INC) -o $@ -c $<

predicates.o: predicates.c hvault.h hdf.h
	gcc $(CFLAGS) $(INC) -o $@ -c $<	

clean:
	rm -rf *.o *.so

install: hvault.so hvault--0.1.sql hvault.control
	install --mode=755 hvault.so $(POSTGRES_PREFIX)/lib
	install --mode=644 hvault--0.1.sql $(POSTGRES_PREFIX)/share/extension
	install --mode=644 hvault.control $(POSTGRES_PREFIX)/share/extension

test: hvault.so test_catalog.sql
	psql gis-test -c "drop extension hvault cascade; create extension hvault;"
	psql gis-test < test_catalog.sql
	psql gis-test
