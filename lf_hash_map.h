#include "lfht.h"

template <class Key, class Value, 
          class HashFunc = THash<Key>, class EqualFunc = TEqualTo<Key>, 
          class Alloc = DEFAULT_ALLOCATOR(Key)>
class TLockFreeHashMap {
private:
    class TPntHashFunc {
    private:
        HashFunc Hash;
    public:
        TPntHashFunc(const HashFunc& hash = HashFunc()) :
            Hash(hash)
        {
        }
        size_t operator () (TAtomicBase key) {
            return Hash(static_cast<Key*>(key));
        }
    };

    class TPntEqualFunc {
    private:
        EqualFunc AreEqual;
    public:
        TPntEqualFunc(const EqualFunc& areEqual = EqualFunc()) :
            AreEqual(areEqual)
        {
        }
        bool operator () (TAtomicBase left, TAtomicBase right) {
            return AreEqual(static_cast<Key*>(left), static_cast<Key*>(right));
        }    
    };

    typedef size_t TSizeType;
    typedef TPair<Key, Value> TValueType;

    typedef int TIterator;
    typedef int TConstIterator;

    typedef TLFHashTable<TPntHashFunc, TPntEqualFunc, 0, 1, 0> TLFHashTableT;
    typedef typename Alloc::template rebind<TLFHashTableT>::other TLFHashTableAllocator;

    TLFHashTableAllocator LFHTAllocator;

    TLFHashTableT* Impl;
public:
    template <class TAllocParam>
    TLockFreeHashMap(TAllocParam* param) : 
        LFHTAllocator(param)
    {
        TLockFreeHashMap();
    }

    explicit
    TLockFreeHashMap(TSizeType size = 4, 
                     const HashFunc& hash = HashFunc(), 
                     const EqualFunc& areEqual = EqualFunc()) {
        Impl = LFHTAllocator.allocate(1);
        new (Impl) TLFHashTableT(0.3, size, TPntHashFunc(hash), TPntEqualFunc(areEqual));
    } 

    template <class TInputIterator>
    TLockFreeHashMap(TInputIterator first, TInputIterator last,
                      TSizeType size = 4, 
                      const HashFunc& hash = HashFunc(), const EqualFunc& areEqual = EqualFunc()) {
        TLockFreeHashMap(size, hash, areEqual);
        Insert(first, last);
    }

    virtual ~TLockFreeHashMap() {
        LFHTAllocator.deallocate(Impl, 1);
    }

    // O(number of elements) working time!
    TSizeType Size();
    TSizeType MaxSize() {
        return TSizeType(-1);
    }
    // O(number of elements) working time!
    bool Empty() {
        return Size() == 0;
    }

    // All iterators are NOT thread-safe.
    TIterator Begin();
    TIterator End();
    TConstIterator Begin() const;
    TConstIterator End() const;

    template <class TInputIterator>
    void Insert(TInputIterator first, TInputIterator last);
};
