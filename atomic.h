#pragma once

#include <stdint.h>
#include <cassert>
#include <pthread.h>

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

inline unsigned AtomicExchange32(volatile void *ptr, unsigned x)
{
        __asm__ __volatile__("xchgl %0,%1"
                                :"=r" ((unsigned) x)
                                :"m" (*(volatile unsigned *)ptr), "0" (x)
                                :"memory");
        return x;
}

inline unsigned AtomicExchange8(volatile void *ptr, unsigned char x)
{
        __asm__ __volatile__("xchgb %0,%1"
                                :"=r" ((unsigned char) x)
                                :"m" (*(volatile unsigned char *)ptr), "0" (x)
                                :"memory");
        return x;
}

#include <linux/futex.h>
#define SYS_futex 202

inline int SysFutex(int* futex, int op, int val1, struct timespec* timeout, void* addr2, int val3)
{
    assert(reinterpret_cast<int64_t>(futex) % 4 == 0); // pointer must by 4 byte aligned
    return syscall(SYS_futex, futex, op, val1, timeout, addr2, val3);
}

inline int SysFutexWait(int* futex, int val1, struct timespec* timeout=NULL)
{
    int res = SysFutex(futex, FUTEX_WAIT_PRIVATE, val1, timeout, NULL, 0);
    // Sometimes on signal, thread can wake up with EPREM instead of EINTR, which is fine.
    assert(res == 0 || res == -EINTR || res == -EWOULDBLOCK || res == -EPERM || (timeout && res == -ETIMEDOUT));
    return res;
}

inline int SysFutexWake(int* futex, int threads)
{
    int res = SysFutex(futex, FUTEX_WAKE_PRIVATE, threads, NULL, NULL, 0);
    assert(res >= 0);
    assert(res <= threads);
    return res;
}

class Mutex
{
private:
    enum { UNLOCKED = 0, LOCKED_UNCONTENDED = 1, CONTENDED = 0x101 };

    union
    {
        int m_state;
        char m_locked;
    };

public:
    Mutex()
    {
        m_state = UNLOCKED;
    }

    bool TryLock()
    {
        return 0 == AtomicExchange8(&m_locked, 1);
    }

    void Lock()
    {
        if (TryLock())
            return;

        while (UNLOCKED != AtomicExchange32(&m_state, CONTENDED))
        {
            SysFutexWait(&m_state, CONTENDED);
        }
    }

    void UnLock()
    {
        assert(UNLOCKED != m_state);
        if (LOCKED_UNCONTENDED != AtomicExchange32(&m_state, UNLOCKED))
        {
            SysFutexWake(&m_state, 1);
        }
    }

    ~Mutex()
    {
        assert(UNLOCKED == m_state);
    }
};

template<typename T>
class Guard
{
private:
    T& m_cond;

public:
    Guard(T& cond)
        : m_cond(cond)
    {
        m_cond.Lock();
    }

    ~Guard()
    {
        m_cond.UnLock();
    }

};
