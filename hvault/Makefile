PG_CONFIG=pg_config
PG_PKGLIBDIR=$(shell $(PG_CONFIG) --pkglibdir)
PG_EXTDIR=$(shell $(PG_CONFIG) --sharedir)/extension

INC=-I$(shell $(PG_CONFIG) --includedir-server)
HDFLIB=-L/usr/lib{,64}/hdf -lmfhdf -ldf -ljpeg -lz
POSTGIS_LIB=-l:$(shell find $(PG_PKGLIBDIR) -name "postgis-2.*.so" -print -quit)
LIB=-lgdal $(HDFLIB) $(POSTGIS_LIB)

CFLAGS = -std=gnu99 -fPIC -D_POSIX_C_SOURCE -DUSE_ASSERT_CHECKING \
         -g -Wall -Wextra -pedantic
# optimization flags
CFLAGS := $(CFLAGS) -O3 -march=native -UUSE_ASSERT_CHECKING -Wno-extra
	
OBJ = analyze.o catalog.o deparse.o driver.o execute.o grid_intersect.o \
	  hvault.o interpolate.o options.o plan.o predicates.o utils.o \
      drivers/modis_swath.o drivers/gdal.o 

HEADERS = analyze.h catalog.h common.h deparse.h driver.h interpolate.h \
          options.h predicates.h utils.h uthash.h liblwgeom_version.h

hvault.so: $(OBJ)
	$(CC) $(CFLAGS) -shared -Wl,-soname,$@ -o $@ $^ $(LIB)

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) $(INC) -o $@ -c $<

liblwgeom_version.h: liblwgeom_version.c
	$(CC) $(CFLAGS) $(INC) -o liblwgeom_version $<
	./liblwgeom_version > $@

clean:
	rm -rf *.o *.so drivers/*.o liblwgeom_version.h liblwgeom_version

install: hvault.so hvault--0.1.sql hvault.control
	install --mode=755 hvault.so $(PG_PKGLIBDIR)
	install --mode=644 hvault--0.1.sql $(PG_EXTDIR)
	install --mode=644 hvault.control $(PG_EXTDIR)
