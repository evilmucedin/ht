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
    public:
        TGuard* AcquireGuard() {
            return GuardManager.AcquireGuard((size_t)&Guard);
        }

    protected:
        static NLFHT_THREAD_LOCAL TGuard* Guard;
        TGuardManager GuardManager;
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
    class KeyMgr = NLFHT::TDefaultKeyManager<Key>,
    class ValMgr = NLFHT::TDefaultValueManager<Val>
>
class TLFHashTable : public NLFHT::TLFHashTableBase {
public:
    typedef TLFHashTable<Key, Val, KeyCmp, HashFn, ValCmp, Alloc, KeyMgr, ValMgr> TSelf;

    friend class NLFHT::TTable<TSelf>;

    typedef Key TKey;
    typedef Val TValue;
    typedef KeyCmp TKeyComparator;
    typedef HashFn THashFn;
    typedef ValCmp TValueComparator;
    typedef Alloc TAllocator;
    typedef KeyMgr TKeyManager;
    typedef ValMgr TValueManager;

    typedef NLFHT::TKeyTraits<TKey> THTKeyTraits;
    typedef NLFHT::TValueTraits<TValue> THTValueTraits;

    typedef typename NLFHT::THashFunc<TKey, THashFn> THashFunc;
    typedef typename NLFHT::TKeysAreEqual<TKey, TKeyComparator> TKeysAreEqual;
    typedef typename NLFHT::TValuesAreEqual<TValue, TValueComparator> TValuesAreEqual;

    typedef NLFHT::TTable<TSelf> TTable;

    typedef NLFHT::TGuard TGuard;
    typedef NLFHT::TGuardManager TGuardManager;

    typedef typename NLFHT::TEntry<TKey, TValue> TEntry;
    typedef typename NLFHT::TConstIterator<TSelf> TConstIterator;

    typedef typename TAllocator::template rebind<TTable> TTableAllocator;
    typedef typename TAllocator::template rebind<TKey> TKeyAllocator;
    typedef typename TAllocator::template rebind<TValue> TValueAllocator;

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
                 const KeyCmp& keysAreEqual = KeyCmp(),
                 const HashFn& hash = HashFn(),
                 const ValCmp& valuesAreEqual = ValCmp(),
                 KeyMgr* keyMgr = 0,
                 ValMgr* valMgr = 0);
    ~TLFHashTable();

    // return NotFound value if there is no such key
    TValue Get(const TKey& key, TSearchHint* hint = 0);
    // NotFound value getter to compare with
    inline static TValue NotFound() {
        return ValueNone();
    }

    // returns true if condition was matched
    bool Put(const TKey& key, const TValue& value, TSearchHint* hint = 0, const TPutCondition& condition = TPutCondition());
    bool PutIfMatch(const TKey& key, const TValue& newValue, const TValue& oldValue, TSearchHint *hint = 0);
    bool PutIfAbsent(const TKey& key, const TValue& value, TSearchHint* hint = 0);
    bool PutIfExists(const TKey& key, const TValue& value, TSearchHint* hint = 0);

    // returns true if key was really deleted
    bool Delete(const TKey& key, TSearchHint* hint = 0);
    bool DeleteIfMatch(const TKey& key, const TValue& oldValue, TSearchHint* hint = 0);

    // iterator related stuff
    size_t Size() const;
    TConstIterator Begin() const;

    TKeyComparator GetKeyComparator() {
        return KeysAreEqual.GetImpl();
    }
    TValueComparator GetValueComparator() {
        return ValuesAreEqual.GetImpl();
    }

    TKeyManager& KeyManagerRef() {
        return *KeyManager;
    }
    TValueManager& ValueManagerRef() {
        return *ValueManager;
    }

    virtual void RegisterThread() {
        NLFHT::TThreadGuardTable::RegisterTable(this);
        KeyManager->RegisterThread();
        ValueManager->RegisterThread();
    }
    virtual void ForgetThread() {
        ValueManager->ForgetThread();
        KeyManager->ForgetThread();
        NLFHT::TThreadGuardTable::ForgetTable(this);
    }


    // JUST TO DEBUG
    void Print(std::ostream& ostr);
    void PrintStatistics(std::ostream& str) {
        GuardManager.PrintStatistics(str);
    }

private:
    // functors
    THashFunc Hash;
    TKeysAreEqual KeysAreEqual;
    TValuesAreEqual ValuesAreEqual;

    // allocators
    TTableAllocator TableAllocator;
    TKeyAllocator KeyAllocator;
    TValueAllocator ValueAllocator;

    // managers
    // can't make them TAutoPtr's, cause they should be destroyed after destructor
    TKeyManager* KeyManager;
    TValueManager* ValueManager;

    // number of head table (in fact it can be less or equal)
    Atomic TableNumber;
    // number of head table of ToDelete list (in fact it can be greater or equal)
    Atomic TableToDeleteNumber;

    // whole table structure
    TTable *volatile Head;
    TTable *volatile ToDelete;

    double Density;

    // TO DEBUG LEAKS
    Atomic TablesCreated;
    Atomic TablesDeleted;

private:
    // thread-safefy and lock-free memory reclamation is done here
    inline void StopGuarding();
    inline void StartGuarding(TSearchHint* hint);
    void TryToDelete();

    // traits wrappers
    static TKey KeyNone() {
        return THTKeyTraits::None();
    }
    void UnRefKey(const TKey& key, size_t cnt = 1) {
        KeyManager->UnRef(key, cnt);
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
    void UnRefValue(const TValue& value, size_t cnt = 1) {
        ValueManager->UnRef(value, cnt);
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

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::TLFHashTable(size_t initialSize, double density,
                                 const TKeyComparator& keysAreEqual,
                                 const THashFn& hash,
                                 const TValueComparator& valuesAreEqual,
                                 TKeyManager* keyManager,
                                 TValueManager* valueManager)
    : Hash(hash)
    , KeysAreEqual(keysAreEqual)
    , ValuesAreEqual(valuesAreEqual)
    , KeyManager(keyManager)
    , ValueManager(valueManager)
    , TableNumber(0)
    , TableToDeleteNumber(std::numeric_limits<AtomicBase>::max())
    , ToDelete(0)
    , Density(density)
    , TablesCreated(0)
    , TablesDeleted(0)
{
    assert(Density > 1e-9);
    assert(Density < 1.);

    if (!KeyManager)
        KeyManager = new TKeyManager();
    if (!ValueManager)
        ValueManager = new TValueManager();

    Head = new TTable(this, initialSize/Density);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::~TLFHashTable() {
    {
        TLFHTRegistration registration(*this);
        while (Head) {
            TTable* tmp = Head;
            Head = Head->Next;
            delete tmp;
        }
        while (ToDelete) {
            TTable* tmp = ToDelete;
            ToDelete = ToDelete->NextToDelete;
            delete tmp;
        }
    }
    delete KeyManager;
    delete ValueManager;

#ifndef NDEBUG
    if (TablesCreated != TablesDeleted) {
        std::cerr << "TablesCreated " << TablesCreated << '\n'
             << "TablesDeleted " << TablesDeleted << '\n';
        VERIFY(false, "Some table lost\n");
    }
#endif
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
typename TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::TValue
TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::Get(const TKey& key, TSearchHint* hint) {
    assert(!KeysAreEqual(key, KeyNone()));

    // Value of Guard should be saved on the stack and then restored.
    // The reason - this method can be called by outer LFH table.
    TGuard* lastGuard = Guard;
    StartGuarding(hint);
    OnGet();

    TTable* startTable = Head;

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

    StopGuarding();
    Guard = lastGuard;

    return returnValue;
}

// returns true if new key appeared in a table
template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
Put(const TKey& key, const TValue& value, TSearchHint* hint, const TPutCondition& cond) {
    assert(THTValueTraits::IsGood(value));
    assert(!KeysAreEqual(key, KeyNone()));

    TGuard* lastGuard = Guard;
    StartGuarding(hint);
    OnPut();

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

    if (!keyInstalled)
        UnRefKey(key);
    if (result == TTable::FAILED)
        UnRefValue(value);

    StopGuarding();
    Guard = lastGuard;
    TryToDelete();

    return result == TTable::SUCCEEDED;
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
PutIfMatch(const TKey& key, const TValue& newValue, const TValue& oldValue, TSearchHint* hint) {
    return Put(key, newValue, hint, TPutCondition(TPutCondition::IF_MATCHES, oldValue));
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
PutIfAbsent(const TKey& key, const TValue& value, TSearchHint* hint) {
    return Put(key, value, hint, TPutCondition(TPutCondition::IF_ABSENT, ValueBaby()));
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::
PutIfExists(const TKey& key, const TValue& newValue, TSearchHint* hint) {
    return Put(key, newValue, hint, TPutCondition(TPutCondition::IF_EXISTS));
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::Delete(const TKey& key, TSearchHint* hint) {
    return Put(key, ValueNone(), hint, TPutCondition(TPutCondition::IF_EXISTS));
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::DeleteIfMatch(const TKey& key, const TValue& oldValue, TSearchHint* hint) {
    return Put(key, ValueNone(), hint, TPutCondition(TPutCondition::IF_MATCHES, oldValue));
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
inline void TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::StartGuarding(TSearchHint* hint) {
    if (hint) {
        if (EXPECT_FALSE(!hint->Guard))
            hint->Guard = NLFHT::TThreadGuardTable::ForTable(this);
        Guard = hint->Guard;
    } else {
        Guard = NLFHT::TThreadGuardTable::ForTable(this);
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
    TTable* toDel = ToDelete;
    if (!toDel)
        return;
    TTable* oldHead = Head;
    AtomicBase firstGuardedTable = GuardManager.GetFirstGuardedTable();

    // if the following is true, it means that no thread works
    // with the tables to ToDelete list
    if (TableToDeleteNumber < firstGuardedTable) {
        if (AtomicCas(&ToDelete, (TTable*)0, toDel)) {
            if (Head == oldHead) {
                while (toDel) {
                    TTable* nextToDel = toDel->NextToDelete;
                    delete toDel;
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
                    TTable* oldToDelete = ToDelete;
                    tail->NextToDelete = oldToDelete;
                    if (AtomicCas(&ToDelete, head, oldToDelete))
                        break;
                }
            }
        }
    }
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
typename TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::TConstIterator
TLFHashTable<K, V, KC, HF, VC, A, KM, VM>::Begin() const {
    TConstIterator begin(Head, -1);
    ++begin;
    return begin;
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

    buf << "ToDelete: " << (size_t)ToDelete << '\n';
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
