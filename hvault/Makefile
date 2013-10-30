PG_CONFIG=pg_config
PG_PKGLIBDIR=$(shell $(PG_CONFIG) --pkglibdir)
PG_EXTDIR=$(shell $(PG_CONFIG) --sharedir)/extension

INC=-I$(shell $(PG_CONFIG) --includedir-server)
HDFLIB=-L/usr/lib{,64}/hdf -lmfhdf -ldf -ljpeg -lz 
LIB=-l:$(PG_PKGLIBDIR)/postgis-2.0.so $(HDFLIB)

CFLAGS = -std=gnu99 -fPIC -D_POSIX_C_SOURCE -DUSE_ASSERT_CHECKING \
         -g -Wall -Wextra -pedantic
# optimization flags
CFLAGS := $(CFLAGS) -O3 -march=native -UUSE_ASSERT_CHECKING -Wno-extra
	
OBJ = analyze.o catalog.o deparse.o driver.o execute.o hvault.o interpolate.o \
      metaload.o options.o plan.o predicates.o utils.o \
      drivers/modis_swath.o 

HEADERS = analyze.h catalog.h common.h deparse.h driver.h interpolate.h \
          options.h predicates.h utils.h uthash.h

hvault.so: $(OBJ)
	$(CC) $(CFLAGS) -shared -Wl,-soname,$@ -o $@ $^ $(LIB)

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) $(INC) -o $@ -c $<

clean:
	rm -rf *.o *.so drivers/*.o

install: hvault.so hvault--0.1.sql hvault.control
	install --mode=755 hvault.so $(PG_PKGLIBDIR)
	install --mode=644 hvault--0.1.sql $(PG_EXTDIR)
	install --mode=644 hvault.control $(PG_EXTDIR)
