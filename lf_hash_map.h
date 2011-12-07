#include "lfht.h"

template < class Key, class Value,
          class HashFunc = HashF<Key>, class EqualFunc = EqualToF<Key>,
          class Alloc = std::allocator<Key> >
class LockFreeHashMap
{
private:
    class PntHashFunc
    {
    private:
        HashFunc Hash;
    public:
        PntHashFunc(const HashFunc& hash = HashFunc()) :
            Hash(hash)
        {
        }
        size_t operator () (AtomicBase key) {
            return Hash(static_cast<Key*>(key));
        }
    };

    class PntEqualFunc {
    private:
        EqualFunc AreEqual;
    public:
        PntEqualFunc(const EqualFunc& areEqual = EqualFunc()) :
            AreEqual(areEqual)
        {
        }
        bool operator () (AtomicBase left, AtomicBase right) {
            return AreEqual(static_cast<Key*>(left), static_cast<Key*>(right));
        }    
    };

    typedef size_t SizeType;
    typedef std::pair<Key, Value> ValueType;

    typedef int Iterator;
    typedef int ConstIterator;

    typedef LFHashTable<PntHashFunc, PntEqualFunc, 0, 1, 0> LFHashTableT;
    typedef typename Alloc::template rebind<LFHashTableT>::other LFHashTableAllocator;

    LFHashTableAllocator m_LFHTAllocator;

    LFHashTableT* m_impl;

public:
    template <class AllocParam>
    LockFreeHashMap(AllocParam* param)
        : m_LFHTAllocator(param)
    {
        LockFreeHashMap();
    }

    explicit
    LockFreeHashMap(SizeType size = 4,
                     const HashFunc& hash = HashFunc(), 
                     const EqualFunc& areEqual = EqualFunc()) {
        m_impl = m_LFHTAllocator.allocate(1);
        new (m_impl) LFHashTableT(0.3, size, PntHashFunc(hash), PntEqualFunc(areEqual));
    } 

    template <class TInputIterator>
    LockFreeHashMap(TInputIterator first, TInputIterator last,
                      SizeType size = 4,
                      const HashFunc& hash = HashFunc(), const EqualFunc& areEqual = EqualFunc()) {
        LockFreeHashMap(size, hash, areEqual);
        Insert(first, last);
    }

    virtual ~LockFreeHashMap() {
        m_LFHTAllocator.deallocate(m_impl, 1);
    }

    // O(number of elements) working time!
    SizeType Size();
    SizeType MaxSize() {
        return SizeType(-1);
    }
    // O(number of elements) working time!
    bool Empty() {
        return Size() == 0;
    }

    // All iterators are NOT thread-safe.
    Iterator Begin();
    Iterator End();
    ConstIterator Begin() const;
    ConstIterator End() const;

    template <class InputIterator>
    void Insert(InputIterator first, InputIterator last);
};
