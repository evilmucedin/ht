CXXX = g++ -std=c++0x -march=native

all: debug

lfht.o: lfht.h atomic.h
	$(CXXX) lfht.cpp -o lfht.o -c

guards.o: guards.h guards.cpp atomic.h
	$(CXXX) guards.cpp -o guards.o -c

time_hash_map.o: time_hash_map.cpp table.h atomic.h mutexht.h lfht.h guards.h atomic_traits.h
	$(CXXX) time_hash_map.cpp -o time_hash_map.o -c

atomic_traits.o: atomic_traits.cpp atomic_traits.h
	$(CXXX) atomic_traits.cpp -o atomic_traits.o -c

test: time_hash_map.o atomic_traits.o guards.o lfht.o
	$(CXXX) atomic_traits.o time_hash_map.o guards.o lfht.o -o test -lrt -lpthread

debug: CXXX += -DDEBUG -g
debug: test

release: CXXX += -DNDEBUG -O3
release: test

clean:
	rm -f time_hash_map.o lfht.o guards.o atomic_traits.o lfht.o test
