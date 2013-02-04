hvault: hvault.c
	gcc -g -Wall -pedantic -shared \
	    -I/usr/pgsql-9.2/include/server/ -o hvault.so hvault.c
	psql gis-test -c "drop extension hvault; create extension hvault;"

clean:
	rm -rf *.o *.so
