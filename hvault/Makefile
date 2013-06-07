POSTGRES_PREFIX=/usr/pgsql-9.2

INC=-I$(POSTGRES_PREFIX)/include/server/
HDFLIB=-L/usr/lib{,64}/hdf -lmfhdf -ldf -ljpeg -lz 
LIB=-l:$(POSTGRES_PREFIX)/lib/postgis-2.0.so $(HDFLIB)
	
CFLAGS = -fPIC -D_POSIX_C_SOURCE -DUSE_ASSERT_CHECKING \
	-g -Wall -pedantic -Wno-long-long
		 
# optimization flags
CFLAGS := $(CFLAGS) -O3 -march=native -UUSE_ASSERT_CHECKING

OBJ = analyze.o catalog.o deparse.o execute.o hdf.o hvault.o interpolate.o \
      metaload.o options.o plan.o predicates.o utils.o 
HEADERS = analyze.h catalog.h common.h deparse.h execute.h hdf.h interpolate.h \
          options.h utils.h

hvault.so: $(OBJ)
	gcc $(CFLAGS) -shared -Wl,-soname,$@ -o $@ $^ $(LIB)

%.o: %.c $(HEADERS)
	gcc $(CFLAGS) $(INC) -o $@ -c $<

clean:
	rm -rf *.o *.so

install: hvault.so hvault--0.1.sql hvault.control
	install --mode=755 hvault.so $(POSTGRES_PREFIX)/lib
	install --mode=644 hvault--0.1.sql $(POSTGRES_PREFIX)/share/extension
	install --mode=644 hvault.control $(POSTGRES_PREFIX)/share/extension
