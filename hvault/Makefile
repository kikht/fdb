INC=-I/usr/pgsql-9.2/include/server/
LIB=-l:/usr/pgsql-9.2/lib/postgis-2.0.so \
	-L/usr/lib/hdf -lmfhdf -ldf -ljpeg -lz 
# CFLAGS=-O3 -flto -march=native -Wall -pedantic
CFLAGS=-g -Wall -pedantic

hvault: hvault.so
	psql gis-test -c "drop extension hvault; create extension hvault;"

hvault.so: hvault.o
	gcc $(CFLAGS) -shared -Wl,-soname,hvault.so -o hvault.so hvault.o $(LIB)

hvault.o: hvault.c hvault.h
	gcc $(CFLAGS) $(INC) -o hvault.o -c hvault.c

clean:
	rm -rf *.o *.so
