#pragma once

#include <stdint.h>
#include <cassert>

typedef volatile intptr_t Atomic;
typedef intptr_t AtomicBase;

static inline bool AtomicCas(Atomic* a, AtomicBase exchange, AtomicBase compare)
{
    return __sync_bool_compare_and_swap(a, compare, exchange);
}

template<typename T>
static inline bool AtomicCas(T* volatile* a, T* exchange, T* compare)
{
    return AtomicCas((AtomicBase*)a, (AtomicBase)exchange, (AtomicBase)compare);
}

static inline AtomicBase AtomicAdd(Atomic& p, AtomicBase delta)
{
    return __sync_add_and_fetch(&p, delta);
}

static inline AtomicBase AtomicIncrement(Atomic& p)
{
    return __sync_add_and_fetch(&p, 1);
}

static inline AtomicBase AtomicDecrement(Atomic& p)
{
    return __sync_add_and_fetch(&p, -1);
}

static inline void AtomicBarrier()
{
    __sync_synchronize();
}

class SpinLock
{
private:
    static inline void SpinLockPause()
    {
        __asm __volatile("pause");
    }

    static inline bool AtomicTryLock(Atomic* a)
    {
        return AtomicCas(a, 1, 0);
    }

    static inline bool AtomicTryAndTryLock(Atomic* a)
    {
        return (*a == 0) && AtomicTryLock(a);
    }

    static inline void AtomicUnlock(Atomic* a)
    {
        __asm__ __volatile__("" : : : "memory");
        *a = 0;
    }

public:
    inline SpinLock() throw()
        : m_val(0)
    {

    }

    inline void Acquire() throw()
    {
        if (!AtomicTryLock(&m_val))
        {
            do
            {
                SpinLockPause();
            } while (!AtomicTryAndTryLock(&m_val));
        }
    }

    inline void Release() throw()
    {
        AtomicUnlock(&m_val);
    }

private:
    Atomic m_val;
};

#define VERIFY(X, Y) assert((X) && (Y))

template<typename T>
T Min(T x, T y)
{
    return (x < y) ? x : y;
}

template<typename T>
struct HashF
{
    size_t operator()(const T& value) const
    {
        return value;
    }
};

template<typename T>
struct EqualToF
{
    bool operator()(const T& a, const T& b) const
    {
        return a == b;
    }
};
