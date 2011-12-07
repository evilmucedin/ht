all: time_hash_map

time_hash_map: time_hash_map.cpp
	g++ time_hash_map.cpp -O2 -std=c++0x -I.. -o time_hash_map.bin -lrt -lpthread
