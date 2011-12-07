#pragma once

#include <stdint.h>
#include <cassert>

typedef volatile intptr_t Atomic;
typedef intptr_t AtomicBase;

static inline bool AtomicCas(Atomic* a, intptr_t exchange, intptr_t compare)
{
    return __sync_bool_compare_and_swap(a, compare, exchange);
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

#define VERIFY(X, Y) assert((X) && !(Y))

template<typename T>
T Min(T x, T y)
{
    return (x < y) ? x : y;
}
