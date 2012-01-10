HOSTNAME = $(shell hostname)
ifeq ($(HOSTNAME), denplusplus-desktop)
    GPATH = /home/denplusplus/gcc46/bin/g++
else
    ifeq ($(HOSTNAME), denplusplus-osx-2.local)
        GPATH = /opt/local/bin/g++-mp-4.6
    else
        ifeq ($(HOSTNAME), memcompute3)
            GPATH = /usr/bin/g++-4.6
        else
            ifeq ($(HOSTNAME), memcompute4)
	        GPATH = /usr/bin/g++-4.6
            else
                GPATH = /usr/bin/g++
            endif
        endif
    endif
endif
CXXX = $(GPATH) -std=c++0x

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

RELEASE_OPTS = -DNDEBUG -O3 -march=native -s

release: CXXX += $(RELEASE_OPTS)
release: test

release-profile: CXXX += $(RELEASE_OPTS) -fprofile-use
release-profile: test

valgrind: CXXX += -DNDEBUG -O2 -g
valgrind: test

profile: CXXX += $(RELEASE_OPTS) -fprofile-generate
profile: test

clean:
	/bin/rm -f time_hash_map.o lfht.o guards.o atomic_traits.o lfht.o test
