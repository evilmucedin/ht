///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2007 - 2011 60East Technologies, LLC, All Rights Reserved
//
// Module Name   : time_hash_map.cpp
// Version       : $Id$
// Project       :
// Description   :
// Author        : Jeffrey M. Birnbaum (jmbnyc@gmail.com)
// Build         : g++ time_hash_map.cpp -O2 -std=c++0x -I.. -o time_hash_map.bin -lrt -lpthread
//
///////////////////////////////////////////////////////////////////////////////

#include "lfht.h"
#include "mutexht.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/variate_generator.hpp>
#include <map>
#include <vector>
#include <thread>
#include <memory>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <iostream>
#include <fstream>

static const int DUMP = 1;

namespace timer
{
// need to link with '-lrt'
class clock_timer
{
public:
    clock_timer(void);
    ~clock_timer(void);

    double elapsedTime(void) const;

    void reset(void);
    void start(void);
    void stop(void);

    long long absoluteTime(void) const;

private:
    clock_timer(const clock_timer&);
    clock_timer& operator=(const clock_timer&);

    long long  _startTime;
    long long  _stopTime;
    bool       _isStopped;
};
clock_timer::clock_timer(void)
    : _startTime(0),
      _stopTime(0),
      _isStopped(true)
{;}

clock_timer::~clock_timer(void)
{;}

double clock_timer::elapsedTime(void) const
{
    return ((_isStopped ? _stopTime : absoluteTime()) - _startTime) * 1E-9;
}

void clock_timer::reset(void)
{
    _startTime = 0;
    _stopTime  = 0;
    _isStopped = true;
}

void clock_timer::start(void)
{
    _startTime = absoluteTime();
    _isStopped = false;
}

void clock_timer::stop(void)
{
    _stopTime = absoluteTime();
    _isStopped = true;
}

long long clock_timer::absoluteTime(void) const
{
    struct timespec ts;
    ::clock_gettime(CLOCK_REALTIME,&ts);
    return static_cast<long long>(1000000000UL) * static_cast<long long>(ts.tv_sec) + static_cast<long long>(ts.tv_nsec);
}
}

namespace pthread
{
// need to link with '-lpthread'
class barrier
{
public:
    barrier(void)
    {;}

    barrier(int count_)
    { ::pthread_barrier_init(&_barrier,0,count_); }

    ~barrier(void)
    { ::pthread_barrier_destroy(&_barrier); }

    void init(int count_)
    { ::pthread_barrier_init(&_barrier,0,count_); }

    void wait(void)
    { ::pthread_barrier_wait(&_barrier); }

private:
    barrier(const barrier&);
    barrier& operator=(const barrier&);

    pthread_barrier_t _barrier;
};
}

namespace cpuid
{
struct cpu_info
{
    size_t      _nSockets;
    size_t      _nPhysicalCores;
    size_t      _nVirtualCores;
    std::string _modelName;
};

namespace _cpuid_detail
{
struct physical_cpu_info
{
    size_t _physicalId;
    size_t _virtualCores;
    size_t _physicalCores;
};

cpu_info cpuInfo(void)
{
    std::string line;
    // get system cpuinfo
    std::ifstream cpuFile("/proc/cpuinfo");
    bool done = false;

    // physical id to cpu info
    typedef std::map<size_t,physical_cpu_info> cpu_map;

    cpu_map cpuMap;
    size_t physicalId = 0;
    size_t siblings = 0;
    size_t cpuCores = 0;
    std::string modelName;

    while (std::getline(cpuFile,line) && done == false)
    {
        if (line.length() != 0)
        {
            if (line.find("physical id") == 0)
            {
                ::sscanf(line.data(),"physical id : %lu",&physicalId);
            }
            else if (line.find("siblings") == 0)
            {
                ::sscanf(line.data(),"siblings : %lu",&siblings);
            }
            else if (line.find("cpu cores") == 0)
            {
                ::sscanf(line.data(),"cpu cores : %lu",&cpuCores);
                physical_cpu_info physicalCpuInfo = { physicalId, siblings, cpuCores };
                cpuMap[physicalId] = physicalCpuInfo;
            }
            else if (line.find("model name") == 0 && modelName.length() == 0)
            {
                size_t pos = line.find(':');
                modelName = line.substr(pos + 2);
            }
        }
        line = "";
    }

    size_t nPhysicalCores = 0;
    size_t nVirtualCores = 0;
    size_t nSockets = cpuMap.size();

    if (cpuMap.empty() == false)
    {
        cpu_map::const_iterator i = cpuMap.begin();
        cpu_map::const_iterator e = cpuMap.end();

        for (;i != e; ++i)
        {
            nPhysicalCores += (*i).second._physicalCores;
            nVirtualCores += (*i).second._virtualCores;
        }
    }
    cpu_info cpuInfo = { nSockets, nPhysicalCores, nVirtualCores };
    cpuInfo._modelName = modelName;
    return cpuInfo;
}

} // namespace _cpuid_detail

cpu_info cpuInfo(void)
{
    return _cpuid_detail::cpuInfo();
}

} // namespace cpuid

std::vector<size_t> g_keys;

void createInput(size_t n_,unsigned seed_)
{
    boost::mt19937 gen(seed_);
    boost::uniform_int<> dist(1,n_);
    boost::variate_generator<boost::mt19937&, boost::uniform_int<> > generator(gen, dist);

    for (size_t i = 0; i != n_; ++i)
    {
        g_keys.push_back(generator());
    }
}

class elapsed_timer
{
public:
    elapsed_timer(void) {;}
    void reset(void) { _timer.start(); }
    double elapsedTime(void)
    {
        _timer.stop();
        return _timer.elapsedTime();
    }
protected:
    timer::clock_timer _timer;
};

struct lf_hash_map_constants
{
    enum { default_bucket_count = 4096 };     // default number of buckets
    enum { bucket_to_segment_ratio = 128 };   // keep the chain lengths to a reasonable size
    enum { bucket_count_expand_factor = 4 };  // multiplier for bucket expansion
};

typedef TLFHashTable<size_t, size_t> lf_hash_map;
// typedef MutexHashTable<AtomicBase, AtomicBase> lf_hash_map;


// allow customization of basic hash_map ops - use std::map API
template<class MapType> inline void insert_map(MapType& map_,size_t key_) { map_.PutIfAbsent(key_, key_ + 1);  }
template<class MapType> inline bool find_map(MapType& map_,size_t key_) { Atomic dummy; return map_.Find(key_, dummy); }

// lf_hash_map find API is a bit different, i.e. bool find(key,value) where the found value is placed into value
template<> 
inline bool find_map(lf_hash_map& map_,size_t key_) 
{ 
    return map_.Get(key_) != map_.NotFound();
}

static const size_t default_iters = 30000000/DUMP;

static void print_system_info(void) 
{
    cpuid::cpu_info cpuInfo = cpuid::cpuInfo();
    struct utsname u;
    ::uname(&u);

    std::cout << "SYSYEM INFO:"
              << "\n sockets        = " << cpuInfo._nSockets
              << "\n physical cores = " << cpuInfo._nPhysicalCores
              << "\n virtual cores  = " << cpuInfo._nVirtualCores
              << "\n model name     = " << cpuInfo._modelName
              << "\n uname          = " << u.sysname  << " " << u.nodename << " " << u.release  << " " << u.version  << " " << u.machine
              << std::endl
              << std::endl;
}

static void report(char const* title_,double elapsedTime_,size_t iters_) 
{
    //double t = (t_ * 1000 * 1000 * 1000) / iters_;
    std::cout << title_ << " " << elapsedTime_ << " secs" << std::endl;
}

template<class MapType, int Flags>
static void time_map_grow(size_t iters_) 
{
    MapType map;
    TLFHTRegistration registration(map);

    elapsed_timer timer;

    timer.reset();
    for (size_t i = 0; i != iters_; ++i)
    {
        insert_map(map,g_keys[i]);
    }

    report("map_grow",timer.elapsedTime(),iters_);
    std::cout << "size: " << map.Size() << std::endl;
}

template<class MapType,int Flags>
static void time_map_grow_predicted(size_t iters_) 
{
    MapType map(iters_);
    TLFHTRegistration registration(map);
    elapsed_timer timer;


    timer.reset();
    for (size_t i = 0; i != iters_; ++i)
    {
        insert_map(map,g_keys[i]);
    }
    report("map_predict_grow",timer.elapsedTime(),iters_);
}

template<class MapType,int Flags>
static void time_map_find(size_t iters_) 
{
    MapType map;
    TLFHTRegistration registration(map);
    elapsed_timer timer;
    size_t r;
    size_t i;

    for (i = 0; i != iters_; ++i)
    {
        insert_map(map,g_keys[i]);
    }

    r = 1;
    find_map(map,g_keys[0]);
    timer.reset();
    for (i = 0; i != iters_; ++i)
    {
        r ^= find_map(map,g_keys[i]);
    }
    report("map_find",timer.elapsedTime(),iters_);
    std::cout << "r value: " << r << std::endl;
}

template<class MapType,int Flags>
static void time_map_erase(size_t iters_) 
{
    MapType map;
    TLFHTRegistration registration(map);
    elapsed_timer timer;
    size_t i;

    for (i = 0; i != iters_; ++i)
    {
        insert_map(map,g_keys[i]);
    }

    std::cout << "size before erase: " << map.Size() << std::endl;

    timer.reset();
    for (i = 0; i != iters_; ++i)
    {
        map.Delete(g_keys[i]);
    }

    report("map_erase",timer.elapsedTime(),iters_);
    std::cout << "size after erase: " << map.Size() << std::endl;
}

template<class MapType,int Flags>
static void measure_st_map(const std::string& mapString_,size_t nLoops_,size_t iters_) 
{
    for (size_t i = 0; i != nLoops_; ++i)
    {
        std::cout << std::endl << mapString_ << std::endl;
        time_map_grow<MapType,Flags>(iters_);
        time_map_grow_predicted<MapType,Flags>(iters_);
        time_map_find<MapType,Flags>(iters_);
        time_map_erase<MapType,Flags>(iters_);
    }
}

const uint32_t lock_free_test = 0x1;
const uint32_t insert_test = 0x8;
const uint32_t find_test = 0x10;

const size_t N = 25000000/DUMP;

template<class MapType,uint32_t Flags>
void mtTestThreadEntryPoint(MapType& map_,pthread::barrier& barrier_,unsigned seed_,double& elapsedTime_)
{
    TLFHTRegistration registration(map_);
    std::vector<size_t> keys;

    boost::mt19937 gen(seed_);
    boost::uniform_int<> dist(1,N);
    boost::variate_generator<boost::mt19937&, boost::uniform_int<> > generator(gen, dist);

    for (size_t i = 0; i != N; ++i)
    {
        keys.push_back(generator());
    }

    if (!(Flags & insert_test)) find_map(map_,0);
    barrier_.wait();

    elapsed_timer timer;
    timer.reset();

    size_t count = 0;
    if (Flags & lock_free_test)
    {
        for (size_t i = 0; i != N; ++i)
        {
            size_t k = keys[i];
            if (Flags & insert_test)
            {
                insert_map(map_,k);
            }
            else
            {
                count += find_map(map_,k);
            }
        }
    }
    elapsedTime_ = timer.elapsedTime();
}

size_t nThreads = 4;

template<class MapType,int Flags>
void mtTest(MapType& map_,const std::string& test_)
{
    std::vector<std::thread> threads;
    std::vector<double> elapsedTimes;
    pthread::barrier barrier(1 + nThreads);

    threads.resize(nThreads);
    elapsedTimes.resize(nThreads);
    for (size_t i = 0; i != nThreads; ++i)
    {
        threads[i] = std::thread(&mtTestThreadEntryPoint<MapType,Flags>,std::ref(map_),std::ref(barrier),i,std::ref(elapsedTimes[i]));
    }

    barrier.wait();
    elapsed_timer timer;
    timer.reset();

    double totalTime = 0;
    for (size_t i = 0; i != nThreads; ++i)
    {
        threads[i].join();
        totalTime += elapsedTimes[i];
    }
    double avgTime = totalTime / nThreads;
    double opTime = (avgTime * 1000 * 1000 * 1000) / N;

    std::cout << "ELAPSED TIME   = " << timer.elapsedTime() << " secs"
              << "\nOPERATION TIME = " << opTime << " ns"
              << "\nSIZE           = " << map_.Size()
              << std::endl;
}

template<class MapType,int Flags>
void measure_mt_map(const std::string& mapString_)
{
    {
        std::cout << std::endl;
        std::cout << "MAP TYPE = " << mapString_ << std::endl;
        std::cout << "LOCK FREE CONCURRENT INSERT TEST - GROW DYNAMIC" << std::endl;
        MapType map_;
        mtTest<MapType,Flags | insert_test>(map_,mapString_);

        std::cout << std::endl;
        std::cout << "LOCK FREE CONCURRENT FIND TEST" << std::endl;
        mtTest<MapType,Flags>(map_,mapString_);
    }

    {
        MapType map_(N);

        std::cout << std::endl;
        std::cout << "LOCK FREE CONCURRENT INSERT TEST - GROW PREDICTED" << std::endl;
        mtTest<MapType,Flags | insert_test>(map_,mapString_);
    }
}

int main(int argc_,char **argv_)
{
    const size_t NN = N;
    createInput(NN, 1);
    std::cout << "map begin" << std::endl;
    time_map_grow_predicted<lf_hash_map,0>(NN);
    return 0;
    
    print_system_info();
    nThreads = (argc_ == 1) ? 4 : ::atoi(argv_[1]);

    size_t iters = default_iters;
    createInput(iters, 1);

    std::cout << "START WARM UP SYSTEM BEFORE EXECUTING TEST" << std::endl;
    for (size_t i = 0; i != 2; ++i)
    {
        measure_st_map<lf_hash_map,0>("lockfree::lf_hash_map",1,iters);
    }
    std::cout << "END WARM UP SYSTEM BEFORE EXECUTING TEST" << std::endl;

    if (1)
    {
        std::cout << std::endl;
        std::cout << "LOCK FREE CONCURRENCY TEST WITH " << nThreads << " THREADS" << std::endl;

        measure_mt_map<lf_hash_map,lock_free_test>("lockfree::lf_hash_map");
    }

    if (1)
    {
        std::cout << std::endl;
        std::cout << "SINGLE THREAD LOCK FREE TEST" << std::endl;

        measure_st_map<lf_hash_map,0>("lockfree::lf_hash_map",1,iters);
    }

    return 0;
}
