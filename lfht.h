#pragma once

#include "table.h"
#include "guards.h"
#include "managers.h"

#include <cstdlib>
#include <cmath>
#include <limits>
#include <memory>
#include <iostream>

namespace NLFHT {
    class TRegistrable {
    public:
        virtual void RegisterThread() = 0;
        virtual void ForgetThread() = 0;
    };

    class TLFHashTableBase : public TRegistrable, public TGuardable {
    };

    template <template <class> class T>
    class TProxy {
    public:
        template <class U>
        class TRedirected : public T<U> {
        public:
            template <class P>
            TRedirected(P param)
                : T<U>(param)
            {
            }
        };
    };

    template <class T>
    class TGuarding {
    public:
        typedef typename T::TSearchHint TSearchHint;

        TGuarding(T& table, TSearchHint* hint) :
            Table(table)
        {
            Table.StartGuarding(hint);
        }
        ~TGuarding() {
            Table.StopGuarding();
        }
    private:
        T& Table;
    };

    template <class Prt>
    class TConstIterator {
    public:
        friend class Prt::TSelf;

        typedef typename Prt::TSelf TParent;
        typedef typename TParent::TTable TTable;
        typedef typename TTable::TConstIteratorT TTableConstIterator;

        typedef typename TParent::TKey TKey;
        typedef typename TParent::TValue TValue;

        TConstIterator(const TParent* Parent)
            : Impl(Parent->Head->Begin())
        {
        }

        inline TKey Key() const {
            return Impl.Key();
        }

        inline TValue Value() const {
            return Impl.Value();
        }

        TConstIterator& operator ++ (int) {
            NextEntry();
            return *this;
        }
        TConstIterator& operator ++ () {
            NextEntry();
            return *this;
        }

        bool IsValid() const {
            return Impl.IsValid();
        }
    private:
        void NextEntry() {
            Impl++;
            if (!Impl.IsValid()) {
                TTable* NextTable = Impl.GetParent()->GetNext();
                if (NextTable)
                    Impl = NextTable->Begin();
            }
        }
    private:
        TTableConstIterator Impl;
    };
}

class TLFHTRegistration : NonCopyable {
private:
    NLFHT::TRegistrable& Table;

public:
    TLFHTRegistration(NLFHT::TRegistrable& table)
       : Table(table)
    {
        Table.RegisterThread();
    }

    ~TLFHTRegistration() {
        Table.ForgetThread();
    }
};

template <
    typename Key,
    typename Val,
    class KeyCmp = EqualToF<Key>,
    class HashFn = HashF<Key>,
    class ValCmp = EqualToF<Val>,
    class Alloc = DEFAULT_ALLOCATOR(Val),
    class KeyMgr = NLFHT::TProxy<NLFHT::TDefaultKeyManager>,
    class ValMgr = NLFHT::TProxy<NLFHT::TDefaultValueManager>
>
class TLFHashTable : public NLFHT::TLFHashTableBase {
public:
    typedef TLFHashTable<Key, Val, KeyCmp, HashFn, ValCmp, Alloc, KeyMgr, ValMgr> TSelf;

    friend class NLFHT::TGuarding<TSelf>;
    friend class NLFHT::TTable<TSelf>;
    friend class NLFHT::TConstIterator<TSelf>;

    typedef Key TKey;
    typedef Val TValue;
    typedef KeyCmp TKeyComparator;
    typedef HashFn THashFn;
    typedef ValCmp TValueComparator;
    typedef Alloc TAllocator;
    typedef typename KeyMgr::template TRedirected<TSelf> TKeyManager;
    typedef typename ValMgr::template TRedirected<TSelf> TValueManager;

    typedef NLFHT::TKeyTraits<TKey> THTKeyTraits;
    typedef NLFHT::TValueTraits<TValue> THTValueTraits;

    typedef typename NLFHT::THashFunc<TKey, THashFn> THashFunc;
    typedef typename NLFHT::TKeysAreEqual<TKey, TKeyComparator> TKeysAreEqual;
    typedef typename NLFHT::TValuesAreEqual<TValue, TValueComparator> TValuesAreEqual;

    typedef NLFHT::TTable<TSelf> TTable;

    typedef NLFHT::TGuard<TSelf> TGuard;
    typedef NLFHT::TGuardManager<TSelf> TGuardManager;

    typedef typename NLFHT::TEntry<TKey, TValue> TEntry;
    typedef typename NLFHT::TConstIterator<TSelf> TConstIterator;

    typedef typename TAllocator::template rebind<TTable>::other TTableAllocator;

    // class incapsulates CAS possibility
    struct TPutCondition {
        enum EWhenToPut {
            ALWAYS,
            IF_ABSENT, // put if THERE IS NO KEY in table. Can put only NONE in this way.
            IF_EXISTS, // put if THERE IS KEY
            IF_MATCHES, // put if THERE IS KEY and VALUE MATCHES GIVEN ONE

            COPYING // reserved for TTable internal use
        };

        EWhenToPut When;
        TValue Value;

        TPutCondition(EWhenToPut when = ALWAYS, TValue value = ValueNone())
            : When(when)
            , Value(value)
        {
        }

        // TO DEBUG ONLY
        std::string ToString() const {
            std::stringstream tmp;
            if (When == ALWAYS)
                tmp << "ALWAYS";
            else if (When == IF_EXISTS)
                tmp << "IF_EXISTS";
            else if (When == IF_ABSENT)
                tmp << "IF_ABSENT";
            else
                tmp << "IF_MATCHES";
            tmp << " with " << ValueToString(Value);
            return tmp.str();
        }
    };

    class TSearchHint {
        public:
            friend class TLFHashTable<Key, Val, KeyCmp, HashFn, ValCmp, Alloc, KeyMgr, ValMgr>;
            friend class NLFHT::TTable< TLFHashTable<Key, Val, KeyCmp, HashFn, ValCmp, Alloc, KeyMgr, ValMgr> >;

        public:
            TSearchHint()
                : Guard(0)
                , Table(0)
            {
            }

        private:
            TGuard* Guard;

            AtomicBase TableNumber;
            TTable* Table;
            TEntry* Entry;
            bool KeySet;

        private:
            TSearchHint(AtomicBase tableNumber, TTable* table, TEntry* entry, bool keySet)
                : Guard(0)
                , TableNumber(tableNumber)
                , Table(table)
                , Entry(entry)
                , KeySet(keySet)
            {
            }
    };

public:
    TLFHashTable(size_t initialSize = 1, double density = 0.5,
                 const TKeyComparator& keysAreEqual = KeyCmp(),
                 const HashFn& hash = HashFn(),
                 const TValueComparator& valuesAreEqual = ValCmp());
    TLFHashTable(const TLFHashTable& other);

    // NotFound value getter to compare with
    inline static TValue NotFound() {
        return ValueNone();
    }

    // return NotFound value if there is no such key
    TValue Get(TKey key, TSearchHint* hint = 0);
    // returns true if condition was matched
    void Put(TKey key, TValue value, TSearchHint* hint = 0);
    bool PutIfMatch(TKey key, TValue newValue, TValue oldValue, TSearchHint *hint = 0);
    bool PutIfAbsent(TKey key, TValue value, TSearchHint* hint = 0);
    bool PutIfExists(TKey key, TValue value, TSearchHint* hint = 0);

    template <class OtherTable>
    void PutAllFrom(const OtherTable& other);

    // returns true if key was really deleted
    bool Delete(TKey key, TSearchHint* hint = 0);
    bool DeleteIfMatch(TKey key, TValue oldValue, TSearchHint* hint = 0);

    // assume, that StartGuarding and StopGuarding are called by client (by creating TGuarding on stack)
    TValue GetNoGuarding(TKey key, TSearchHint* hint = 0);

    void PutNoGuarding(TKey key, TValue value, TSearchHint* hint = 0);
    bool PutIfMatchNoGuarding(TKey key, TValue newValue, TValue oldValue, TSearchHint *hint = 0);
    bool PutIfAbsentNoGuarding(TKey key, TValue value, TSearchHint* hint = 0);
    bool PutIfExistsNoGuarding(TKey key, TValue value, TSearchHint* hint = 0);

    bool DeleteNoGuarding(TKey key, TSearchHint* hint = 0);
    bool DeleteIfMatchNoGuarding(TKey key, TValue oldValue, TSearchHint* hint = 0);

    // massive operations
    template <class OtherTable>
    void PutAllFromNoGuarding(const OtherTable& other) {
        return ValuesAreEqual.GetImpl();
    }

    size_t Size() const;
    bool Empty() const {
        return Size() == 0;
    }
    TConstIterator Begin() const;

    TKeyComparator GetKeyComparator() const {
        return KeysAreEqual.GetImpl();
    }
    TValueComparator GetValueComparator() const {
        return ValuesAreEqual.GetImpl();
    }
    THashFn GetHashFunction() const {
        return Hash.GetImpl();
    }

    TGuard& GuardRef() {
        return *Guard;
    }
    TGuardManager& GuardManagerRef() {
        return GuardManager;
    }
    TKeyManager& KeyManagerRef() {
        return KeyManager;
    }
    TValueManager& ValueManagerRef() {
        return ValueManager;
    }

    virtual void RegisterThread() {
        NLFHT::TThreadGuardTable::RegisterTable(this);
        KeyManager.RegisterThread();
        ValueManager.RegisterThread();
    }
    virtual void ForgetThread() {
        ValueManager.ForgetThread();
        KeyManager.ForgetThread();
        NLFHT::TThreadGuardTable::ForgetTable(this);
    }
    virtual NLFHT::TBaseGuard* AcquireGuard() {
        return GuardManager.AcquireGuard();
    }

    TTable* GetHead() {
        return Head;
    }
    TTable* GetHeadToDelete() {
        return HeadToDelete;
    }

    // JUST TO DEBUG
    void Print(std::ostream& ostr);
    void PrintStatistics(std::ostream& str) {
        GuardManager.PrintStatistics(str);
    }

private:
    class THeadWrapper : public NLFHT::TVolatilePointerWrapper<TTable> {
    public:
        THeadWrapper(TLFHashTable* parent)
            : Parent(parent)
        {
        }
        inline THeadWrapper& operator= (TTable* table) {
            Set(table);
            return *this;
        }

        ~THeadWrapper() {
            TTable* current = Get();
            while (current) {
                TTable* tmp = current;
                current = current->GetNext();
                Parent->DeleteTable(tmp);
            }
#ifndef NDEBUG
            if (Parent->TablesCreated != Parent->TablesDeleted) {
                std::cerr << "TablesCreated " << Parent->TablesCreated << '\n'
                     << "TablesDeleted " << Parent->TablesDeleted << '\n';
                VERIFY(false, "Some table lost\n");
            }
#endif
        }
    private:
        TLFHashTable* Parent;
    };

    class THeadToDeleteWrapper : public NLFHT::TVolatilePointerWrapper<TTable> {
    public:
        THeadToDeleteWrapper(TLFHashTable* parent)
            : Parent(parent)
        {
        }
        inline THeadToDeleteWrapper& operator= (TTable* table) {
            Set(table);
            return *this;
        }

        ~THeadToDeleteWrapper() {
            TTable* current = Get();
            while (current) {
                TTable* tmp = current;
                current = current->GetNextToDelete();
                Parent->DeleteTable(tmp);
            }
        }
    private:
        TLFHashTable* Parent;
    };

    // is used by TTable
    double Density;

    // functors
    THashFunc Hash;
    TKeysAreEqual KeysAreEqual;
    TValuesAreEqual ValuesAreEqual;

    // allocators
    TTableAllocator TableAllocator;

    // whole table structure
    THeadWrapper Head;
    THeadToDeleteWrapper HeadToDelete;

    // guarding
    static NLFHT_THREAD_LOCAL TGuard* Guard;
    TGuardManager GuardManager;

    // managers
    TKeyManager KeyManager;
    TValueManager ValueManager;

    // number of head table (in fact it can be less or equal)
    Atomic TableNumber;
    // number of head table of HeadToDelete list (in fact it can be greater or equal)
    Atomic TableToDeleteNumber;

#ifndef NDEBUG
    // TO DEBUG LEAKS
    Atomic TablesCreated;
    Atomic TablesDeleted;
#endif

private:
    template <bool ShouldSetGuard>
    TValue GetImpl(const TKey& key, TSearchHint* hint = 0);

    template <bool ShouldSetGuard, bool ShouldDeleteKey>
    bool PutImpl(const TKey& key, const TValue& value, const TPutCondition& condition, TSearchHint* hint = 0);

    // thread-safefy and lock-free memory reclamation is done here
    inline void StopGuarding();
    inline void StartGuarding(TSearchHint* hint);

    void TryToDelete();

    // allocators usage wrappers
    TTable* CreateTable(TLFHashTable* parent, size_t size) {
        TTable* newTable = TableAllocator.allocate(1);
        try {
            new (newTable) TTable(parent, size);
            newTable->AllocSize = size;
            return newTable;
        }
        catch (...) {
            TableAllocator.deallocate(newTable, size);
            throw;
        }
    }
    void DeleteTable(TTable* table, bool shouldDeleteKeys = false) {
#ifdef TRACE
        Trace(Cerr, "DeleteTable %zd\n", (size_t)table);
#endif
        if (shouldDeleteKeys) {
            for (typename TTable::TAllKeysConstIterator it = table->BeginAllKeys(); it.IsValid(); ++it)
                UnRefKey(it.Key());
        }
        TableAllocator.destroy(table);
        TableAllocator.deallocate(table, table->AllocSize);
    }

    // destructing
    void Destroy();

    // traits wrappers
    static TKey KeyNone() {
        return THTKeyTraits::None();
    }
    void UnRefKey(TKey key, size_t cnt = 1) {
        KeyManager.UnRef(key, cnt);
    }

    static TValue ValueNone() {
        return THTValueTraits::None();
    }
    static TValue ValueBaby() {
        return THTValueTraits::Baby();
    }
    static TValue ValueDeleted() {
        return THTValueTraits::Deleted();
    }
    void UnRefValue(TValue value, size_t cnt = 1) {
        ValueManager.UnRef(value, cnt);
    }

    // guard getting wrapper
    TGuard* GuardForTable() {
        return dynamic_cast<TGuard*>(NLFHT::TThreadGuardTable::ForTable(this));
    }

    // JUST TO DEBUG
    static std::string KeyToString(const TKey& key) {
        return NLFHT::KeyToString<TKey>(key);
    }
    static std::string ValueToString(const TValue& value) {
        return NLFHT::ValueToString<TValue>(value);
    }

    void Trace(std::ostream& ostr, const char* format, ...);

    inline void OnPut() {
        Guard->OnGlobalPut();
    }
    inline void OnGet() {
        Guard->OnGlobalGet();
    }
};

// need dirty hacks to avoid problems with macros that accept template as a parameter

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
NLFHT_THREAD_LOCAL NLFHT::TGuard< TLFHashTable<K, V, KC, HF, VC, A, KM, VM> >* TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::Guard((TGuard*)0);

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::TLFHashTable(size_t initialSize, double density,
                                 const TKeyComparator& keysAreEqual,
                                 const THashFn& hash,
                                 const TValueComparator& valuesAreEqual)
    : Density(density)
    , Hash(hash)
    , KeysAreEqual(keysAreEqual)
    , ValuesAreEqual(valuesAreEqual)
    , Head(this)
    , HeadToDelete(this)
    , GuardManager(this)
    , KeyManager(this)
    , ValueManager(this)
    , TableNumber(0)
    , TableToDeleteNumber(std::numeric_limits<AtomicBase>::max())
#ifndef NDEBUG
    , TablesCreated(0)
    , TablesDeleted(0)
#endif
{
    assert(Density > 1e-9);
    assert(Density < 1.);
    assert(initialSize);
    Head = CreateTable(this, initialSize/Density);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::TLFHashTable(const TLFHashTable& other)
    : Density(other.Density)
    , Hash(other.Hash)
    , KeysAreEqual(other.KeysAreEqual)
    , ValuesAreEqual(other.ValuesAreEqual)
    , Head(this)
    , HeadToDelete(this)
    , GuardManager(this)
    , KeyManager(this)
    , ValueManager(this)
    , TableNumber(0)
    , TableToDeleteNumber(std::numeric_limits<AtomicBase>::max())
#ifndef NDEBUG
    , TablesCreated(0)
    , TablesDeleted(0)
#endif
{
#ifdef TRACE
    Trace(Cerr, "TLFHashTable copy constructor called\n");
#endif
    Head = CreateTable(this, Max((size_t)1, other.Size()) / Density);
    PutAllFrom(other);
#ifdef TRACE
    Trace(Cerr, "TFLHashTable copy created\n");
#endif
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
template <bool ShouldSetGuard>
typename TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::TValue
TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::GetImpl(const TKey& key, TSearchHint* hint) {
    assert(!KeysAreEqual(key, KeyNone()));
#ifdef TRACE
    Trace(Cerr, "TLFHashTable.Get(%s)\n", ~KeyToString(key));
#endif

    TGuard* lastGuard;
    if (ShouldSetGuard) {
        // Value of Guard should be saved on the stack and then restored.
        // The reason - this method can be called by outer LFH table.
        lastGuard = Guard;
        StartGuarding(hint);
        OnGet();
    }

#ifdef TRACE
    Trace(Cerr, "Get \"%s\"\n", ~KeyToString(key));
#endif

#ifdef TRACE_GET
    size_t headLen = 0;
    TTable* cur2 = Head;
    while (cur2) {
        Cerr << "-----" << Endl;
        cur2->Print(Cerr, true);
        Cerr << "-----" << Endl;
        ++headLen;
        cur2 = cur2->GetNext();
    }
    size_t deleteLen = 0;
    cur2 = HeadToDelete;
    while (cur2) {
        ++deleteLen;
        cur2 = cur2->NextToDelete;
    }
    Cerr << headLen << " " << deleteLen << Endl;
#endif

    if (EXPECT_FALSE(Head->GetNext()))
        Head->DoCopyTask();

    const size_t hashValue = Hash(key);
    TValue returnValue;
    TTable* cur = Head;
    do {
        if (cur->Get(key, hashValue, returnValue, hint))
            break;
        cur = cur->GetNext();
    } while (cur);

    if (!cur || EXPECT_FALSE(ValuesAreEqual(returnValue, ValueBaby())))
        returnValue = NotFound();

    if (ShouldSetGuard) {
        StopGuarding();
        Guard = lastGuard;
    }

#ifdef TRACE
    Trace(Cerr, "Get returns %s\n", ~ValueToString(returnValue));
#endif
    return returnValue;
}

// returns true if new key appeared in a table
template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
template <bool ShouldSetGuard, bool ShouldDeleteKey>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
PutImpl(const TKey& key, const TValue& value, const TPutCondition& cond, TSearchHint* hint) {
    assert(THTValueTraits::IsGood(value));
    assert(!KeysAreEqual(key, KeyNone()));
#ifdef TRACE
    Trace(Cerr, "TLFHashTable.Put key \"%s\" and value \"%s\" under condition %s..\n",
            ~KeyToString(key), ~ValueToString(value),
            ~cond.ToString());
#endif

    TGuard* lastGuard;
    if (ShouldSetGuard) {
        lastGuard = Guard;
        StartGuarding(hint);
        OnPut();
    }

    if (EXPECT_FALSE(Head->GetNext()))
        Head->DoCopyTask();

    typename TTable::EResult result;
    bool keyInstalled;

    TTable* cur = Head;
    size_t cnt = 0;
    while (true) {
        if (++cnt >= 100000)
            VERIFY(false, "Too long table list\n");
        if ((result = cur->Put(key, value, cond, keyInstalled)) != TTable::FULL_TABLE)
            break;
        if (!cur->GetNext()) {
            cur->CreateNext();
        }
        cur = cur->GetNext();
    }

    if (ShouldDeleteKey && !keyInstalled)
        UnRefKey(key);
    if (result == TTable::FAILED)
        UnRefValue(value);

    if (ShouldSetGuard) {
        StopGuarding();
        Guard = lastGuard;
    }

    TryToDelete();

    return result == TTable::SUCCEEDED;
}

// hash table access methods

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
typename TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::TValue
TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
Get(TKey key, TSearchHint* hint) {
    return GetImpl<true>(key, hint);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
void TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
Put(TKey key, TValue value, TSearchHint* hint) {
    PutImpl<true, true>(key, value, TPutCondition(TPutCondition::IF_MATCHES), hint);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
PutIfMatch(TKey key, TValue newValue, TValue oldValue, TSearchHint* hint) {
    return PutImpl<true, true>(key, newValue, TPutCondition(TPutCondition::IF_MATCHES, oldValue), hint);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
PutIfAbsent(TKey key, TValue value, TSearchHint* hint) {
    return PutImpl<true, true>(key, value, TPutCondition(TPutCondition::IF_ABSENT, ValueBaby()), hint);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
PutIfExists(TKey key, TValue newValue, TSearchHint* hint) {
    return PutImpl<true, true>(key, newValue, TPutCondition(TPutCondition::IF_EXISTS), hint);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::Delete(TKey key, TSearchHint* hint) {
    return PutImpl<true, false>(key, ValueNone(), TPutCondition(TPutCondition::IF_EXISTS), hint);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::DeleteIfMatch(TKey key, TValue oldValue, TSearchHint* hint) {
    return PutImpl<true, false>(key, ValueNone(), TPutCondition(TPutCondition::IF_MATCHES, oldValue), hint);
}

// no guarding

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
typename TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::TValue
TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
GetNoGuarding(TKey key, TSearchHint* hint) {
    return GetImpl<false>(key, hint);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
void TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
PutNoGuarding(TKey key, TValue value, TSearchHint* hint) {
    PutImpl<false, true>(key, value, TPutCondition(), hint);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
PutIfMatchNoGuarding(TKey key, TValue newValue, TValue oldValue, TSearchHint* hint) {
    return PutImpl<false, true>(key, newValue, TPutCondition(TPutCondition::IF_MATCHES, oldValue), hint);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
PutIfAbsentNoGuarding(TKey key, TValue value, TSearchHint* hint) {
    return PutImpl<false, true>(key, value, TPutCondition(TPutCondition::IF_ABSENT, ValueBaby()), hint);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
PutIfExistsNoGuarding(TKey key, TValue newValue, TSearchHint* hint) {
    return PutImpl<false, true>(key, newValue, TPutCondition(TPutCondition::IF_EXISTS), hint);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::DeleteNoGuarding(TKey key, TSearchHint* hint) {
    return PutImpl<false, false>(key, ValueNone(), TPutCondition(TPutCondition::IF_EXISTS), hint);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::DeleteIfMatchNoGuarding(TKey key, TValue oldValue, TSearchHint* hint) {
    return PutImpl<false, false>(key, ValueNone(), TPutCondition(TPutCondition::IF_MATCHES, oldValue), hint);
}

// massive put

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
template <class OtherTable>
void TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::PutAllFrom(const OtherTable& other) {
    TLFHTRegistration registration(*this);
    for (TConstIterator it = other.Begin(); it.IsValid(); it++) {
        TKey keyClone = KeyManager.CloneAndRef(it.Key());
        TKey valueClone = ValueManager.CloneAndRef(it.Value());
        Put(keyClone, valueClone);
    }
}

// how to guarp

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
inline void TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::StartGuarding(TSearchHint* hint) {
    if (hint) {
        if (EXPECT_FALSE(!hint->Guard))
            hint->Guard = GuardForTable();
        Guard = hint->Guard;
    } else {
        Guard = GuardForTable();
    }
    VERIFY(Guard, "Register in table!\n");
    assert(Guard == NLFHT::TThreadGuardTable::ForTable(this));
    assert(Guard->GetThreadId() == CurrentThreadId());

    while (true) {
        AtomicBase currentTableNumber = TableNumber;
        Guard->GuardTable(currentTableNumber);
        AtomicBarrier();
        if (EXPECT_TRUE(TableNumber == currentTableNumber)) {
            // Now we are sure, that no thread can delete current Head.
            return;
        }
    }
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
inline void TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::StopGuarding() {
    assert(Guard);
    Guard->StopGuarding();
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
void TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::TryToDelete() {
    TTable* toDel = HeadToDelete;
    if (!toDel)
        return;
    TTable* oldHead = Head;
    AtomicBase firstGuardedTable = GuardManager.GetFirstGuardedTable();

    // if the following is true, it means that no thread works
    // with the tables to ToDelete list
    if (TableToDeleteNumber < firstGuardedTable) {
        if (AtomicCas(&HeadToDelete, (TTable*)0, toDel)) {
            if (Head == oldHead) {
                while (toDel) {
                    TTable* nextToDel = toDel->NextToDelete;
#ifdef TRACE_MEM
                    Trace(Cerr, "Deleted table %zd of size %zd\n", (size_t)toDel, toDel->Size);
#endif
                    DeleteTable(toDel, true);
                    toDel = nextToDel;
                }
            } else {
                // This is handling of possible ABA problem.
                // If some other table was removed from list,
                // successfull CAS doesn't guarantee that we have
                // the same table as before. We put all the elements back
                // to the ToDelete list.
                TTable* head = toDel;
                TTable* tail = head;
                while (tail->NextToDelete) {
                    tail = tail->NextToDelete;
                }

                while (true) {
                    TTable* oldToDelete = HeadToDelete;
                    tail->NextToDelete = oldToDelete;
                    if (AtomicCas(&HeadToDelete, head, oldToDelete))
                        break;
                }
            }
        }
    }
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
typename TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::TConstIterator
TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::Begin() const {
    return TConstIterator(this);
}

// JUST TO DEBUG

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
void TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::Print(std::ostream& ostr) {
    std::stringstream buf;
    buf << "TLFHashTable printout\n";

    buf << '\n';

    TTable* cur = Head;
    while (cur) {
        cur->Print(buf);
        TTable* next = cur->GetNext();
        if (next)
            buf << "---------------\n";
        cur = next;
    }

    buf << "HeadToDelete: " << (size_t)(TTable*)HeadToDelete << '\n';
    buf << KeyManager->ToString() << '\n'
        << ValueManager->ToString() << '\n';

    ostr << buf.str() << '\n';
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
size_t TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::Size() const {
    size_t result = 0;
    TConstIterator it = Begin();
    while (it.IsValid()) {
        ++result;
        ++it;
    }
    return result;
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
void TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::Trace(std::ostream& ostr, const char* format, ...) {
    char buf1[10000];
    sprintf(buf1, "Thread %zd: ", (size_t)&errno);

    char buf2[10000];
    va_list args;
    va_start(args, format);
    vsprintf(buf2, format, args);
    va_end(args);

    ostr << buf1 << buf2;
}
