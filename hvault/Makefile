hvault: hvault.so
	psql gis-test -c "drop extension hvault; create extension hvault;"

analyze: hvault.c
	clang -I/usr/pgsql-9.2/include/server/ --analyze hvault.c

hvault.so: hvault.o
	gcc -shared -Wl,-soname,hvault.so -o hvault.so hvault.o \
	    -l:/usr/pgsql-9.2/lib/postgis-2.0.so \
	    -L/usr/lib/hdf -lmfhdf -ldf -ljpeg -lz 

hvault.o: hvault.c
	gcc -g -Wall -pedantic -fPIC \
		-I/usr/pgsql-9.2/include/server/ \
		-o hvault.o -c hvault.c

clean:
	rm -rf *.o *.so
