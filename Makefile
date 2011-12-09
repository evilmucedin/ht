CXXX = g++ -std=c++0x -march=native

all: debug

time_hash_map.o: time_hash_map.cpp lfht.h atomic.h
	$(CXXX) time_hash_map.cpp -o time_hash_map.o -c

lfht.o: lfht.cpp lfht.h atomic.h
	$(CXXX) lfht.cpp -o lfht.o -c

test: lfht.o time_hash_map.o
	$(CXXX) time_hash_map.o lfht.o -o test -lrt -lpthread

debug: CXXX += -DDEBUG -g
debug: test

release: CXXX += -DNDEBUG -O3
release: test

clean:
	rm -f time_hash_map.o lfht.o test
