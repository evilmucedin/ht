all: test

time_hash_map.o: time_hash_map.cpp
	g++ time_hash_map.cpp -O3 -std=c++0x -o time_hash_map.o -DNDEBUG -c

lfht.o: lfht.cpp
	g++ lfht.cpp -O3 -std=c++0x -o lfht.o -lrt -lpthread -DNDEBUG -c

test: lfht.o time_hash_map.o
	g++ time_hash_map.o lfht.o -o test -lrt -lpthread
