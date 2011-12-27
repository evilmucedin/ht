#pragma once

#include "table.h"
#include "guards.h"

#include <cstdlib>
#include <cmath>
#include <limits>

namespace NLFHT {
    class TLFHashTableBase {
    public:
        TGuard* AcquireGuard() {
            return GuardManager.AcquireGuard((size_t)&Guard);
        }
    protected:
        static NLFHT_THREAD_LOCAL TGuard* Guard;
        TGuardManager GuardManager;
    };
}

template <
    typename Key, 
    typename Val,
    class KeyCmp = EqualToF<Key>,
    class HashFn = HashF<Key>,
    class ValCmp = EqualToF<Val>
>
class TLFHashTable : public NLFHT::TLFHashTableBase {
public:
    typedef TLFHashTable<Key, Val, KeyCmp, HashFn, ValCmp> TSelf;

    friend class NLFHT::TTable<TSelf>;

    typedef Key TKey;
    typedef Val TValue;
    typedef KeyCmp TKeyCmp;
    typedef HashFn THashFn;
    typedef ValCmp TValCmp;

    typedef NLFHT::TKeyTraits<TKey> THTKeyTraits;
    typedef NLFHT::TValueTraits<TValue> THTValueTraits;

    typedef typename NLFHT::THashFunc<TKey, THashFn> THashFunc; 
    typedef typename NLFHT::TKeysAreEqual<TKey, TKeyCmp> TKeysAreEqual;
    typedef typename NLFHT::TValuesAreEqual<TValue, TValCmp> TValuesAreEqual;

    typedef NLFHT::TTable<TSelf> TTable;

    typedef NLFHT::TGuard TGuard;
    typedef NLFHT::TGuardManager TGuardManager;

    typedef typename NLFHT::TEntry<TKey, TValue> TEntry;
    typedef typename NLFHT::TConstIterator<TSelf> TConstIterator; 

    // class incapsulates CAS possibility
    struct TPutCondition {
        enum EWhenToPut {
            ALWAYS,
            IF_ABSENT, // put if THERE IS NO KEY in table. Can put only NONE in this way.
            IF_EXISTS, // put if THERE IS KEY
            IF_MATCHES // put if THERE IS KEY and VALUE MATCHES GIVEN ONE
        };

        EWhenToPut When;
        TValue Value;

        TPutCondition(EWhenToPut when = ALWAYS, TValue value = ValueNone()) :
            When(when),
            Value(value)
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
            friend class TLFHashTable<Key, Val, KeyCmp, HashFn, ValCmp>;
            friend class NLFHT::TTable< TLFHashTable<Key, Val, KeyCmp, HashFn, ValCmp> >;

        public:
            TSearchHint() :
                Table(0)
            {
            }

        private:
            TGuard* Guard;

            AtomicBase TableNumber;
            TTable* Table;
            TEntry* Entry;
            bool KeySet;
        private:
            TSearchHint(AtomicBase tableNumber, TTable* table, TEntry* entry, bool keySet) :
                TableNumber(tableNumber),
                Table(table),
                Entry(entry),
                KeySet(keySet)
            {
            }
    };

public:
    TLFHashTable(size_t initialSize = 1, double density = 0.5,
                 const KeyCmp& keysAreEqual = KeyCmp(),
                 const HashFn& hash = HashFn(),
                 const ValCmp& valuesAreEqual = ValCmp());
    ~TLFHashTable();

    // return NotFound value if there is no such key
    TValue Get(const TKey& key, TSearchHint* hint = 0);
    // NotFound value getter to compare with
    static TValue NotFound() { return ValueNone(); } 

    // returns true if condition was matched
    bool Put(const TKey& key, const TValue& value, 
             TPutCondition condition = TPutCondition(), TSearchHint* hint = 0);
    bool PutIfMatch(const TKey& key, 
                    const TValue& newValue, const TValue& oldValue,
                    TSearchHint *hint = 0);
    bool PutIfAbsent(const TKey& key, const TValue& value, TSearchHint* hint = 0);
    bool PutIfExists(const TKey& key, const TValue& value, TSearchHint* hint = 0);

    // returns true if key was really deleted
    bool Delete(const TKey& key);
    bool DeleteIfMatch(const TKey& key, const TValue& oldValue);

    // iterator related stuff
    TConstIterator Begin() const;

    // JUST TO DEBUG
    void Print(std::ostream& ostr);
    void PrintStatistics(std::ostream& str) {
        GuardManager.PrintStatistics(str);
    }

    THTKeyTraits& GetKeyTraits() {
        return KeyTraits;
    }
    THTValueTraits& GetValueTraits() {
        return ValueTraits;
    }

    size_t Size() const;

private:
    // functors
    THashFunc Hash;
    TKeysAreEqual KeysAreEqual;
    TValuesAreEqual ValuesAreEqual;
    THTKeyTraits KeyTraits;
    THTValueTraits ValueTraits;

    // number of head table (in fact it can be less or equal)
    Atomic TableNumber;
    // number of head table of ToDelete list (in fact it can be greater or equal) 
    Atomic TableToDeleteNumber;

    // guarded related stuff

    // whole table structure
    TTable *volatile Head;
    TTable *volatile ToDelete;

    double Density;

private:
    // thread-safefy and lock-free memory reclamation is done here
    void StopGuarding();
    void StartGuarding();
    void TryToDelete();

    // traits wrappers
    static TKey KeyNone() {
        return THTValueTraits::None();
    }
    void UnRefKey(const TKey& key, size_t cnt = 1) {
        KeyTraits.UnRef(key, cnt);
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
        ValueTraits.UnRef(value, cnt);
    }

    static std::string KeyToString(const TKey& key) {
        return NLFHT::KeyToString<TKey>(key);
    }
    static std::string ValueToString(const TValue& value) {
        return NLFHT::ValueToString<TValue>(value);
    }

    // JUST TO DEBUG
    void Trace(std::ostream& ostr, const char* format, ...);

    void OnPut() {
        Guard->OnGlobalPut();
    }
    void OnGet() {
        Guard->OnGlobalGet();
    }
};

class TLFHTRegistration {
private:
    NLFHT::TLFHashTableBase* PTable;
public:
    template <class K, class V, class KC, class HF, class VC>
    TLFHTRegistration(TLFHashTable<K, V, KC, HF, VC>& table) :
        PTable(static_cast<NLFHT::TLFHashTableBase*>(&table))           
    {
        NLFHT::TThreadGuardTable::RegisterTable(PTable);
    }
    ~TLFHTRegistration() {
        NLFHT::TThreadGuardTable::ForgetTable(PTable);
    }
};

template <class K, class V, class KC, class HF, class VC>
TLFHashTable<K, V, KC, HF, VC>::TLFHashTable(size_t initialSize, double density,
                                 const TKeyCmp& keysAreEqual,
                                 const THashFn& hash,
                                 const TValCmp& valuesAreEqual)
    : Hash(hash)
    , KeysAreEqual(keysAreEqual)
    , ValuesAreEqual(valuesAreEqual)
    , TableNumber(0)
    , TableToDeleteNumber(std::numeric_limits<AtomicBase>::max())
    , ToDelete(0)
    , Density(density)
{
    assert(Density > 1e-9);
    assert(Density < 1.);
    Head = new TTable(this, initialSize/Density);
    Guard = (NLFHT::TGuard*)0;
}

template <class K, class V, class KC, class HF, class VC>
TLFHashTable<K, V, KC, HF, VC>::~TLFHashTable() {
    while (Head) {
        TTable* tmp = Head;
        Head = Head->Next;
        delete tmp;
    }
    while (ToDelete) {
        TTable* tmp = ToDelete;
        ToDelete = ToDelete->Next;
        delete tmp;
    }
}

template <class K, class V, class KC, class HF, class VC>
typename TLFHashTable<K, V, KC, HF, VC>::TValue
TLFHashTable<K, V, KC, HF, VC>::Get(const TKey& key, TSearchHint* hint) {
    assert(!KeysAreEqual(key, KeyNone()));

    StartGuarding();
    OnGet();

    TTable* startTable = Head;

    size_t hashValue = Hash(key);
    TValue returnValue = NotFound();
    TTable* cur = startTable;
    while (cur && !cur->Get(key, hashValue, returnValue, hint)) {
         cur = cur->GetNext();
    }
    if (ValuesAreEqual(returnValue, ValueNone()) || 
        ValuesAreEqual(returnValue, ValueBaby()))
        returnValue = NotFound();

    StopGuarding();
    return returnValue; 
}

// returns true if new key appeared in a table
template <class K, class V, class KC, class HF, class VC>
bool TLFHashTable<K, V, KC, HF, VC>::
Put(const TKey& key, const TValue& value, TPutCondition cond, TSearchHint* hint) {
    assert(THTValueTraits::IsGood(value));
    assert(!KeysAreEqual(key, KeyNone()));

    StartGuarding();
    OnPut();

    typename TTable::EResult result;
    bool keyInstalled;

    TTable* cur = Head;
    size_t cnt = 0;
    while (true) {
#ifndef DEBUG
        if (++cnt >= 100000)
            VERIFY(false, "Too long table list\n");
#endif
        if ((result = cur->Put(key, value, cond, keyInstalled)) != TTable::FULL_TABLE)
            break;
        if (!cur->GetNext()) {
            cur->CreateNext();
        }
        cur->DoCopyTask();
        cur = cur->GetNext();
    }

    if (!keyInstalled)
        UnRefKey(key);
    if (result == TTable::FAILED)
        UnRefValue(value);

    StopGuarding();
    TryToDelete();

    return result == TTable::SUCCEEDED;
}

template <class K, class V, class KC, class HF, class VC>
bool TLFHashTable<K, V, KC, HF, VC>::
PutIfMatch(const TKey& key, const TValue& newValue, const TValue& oldValue, TSearchHint* hint) {
    return Put(key, newValue, TPutCondition(TPutCondition::IF_MATCHES, oldValue), hint);
}

template <class K, class V, class KC, class HF, class VC>
bool TLFHashTable<K, V, KC, HF, VC>::
PutIfAbsent(const TKey& key, const TValue& value, TSearchHint* hint) {
    return Put(key, value, TPutCondition(TPutCondition::IF_ABSENT, ValueBaby()), hint);
}

template <class K, class V, class KC, class HF, class VC>
bool TLFHashTable<K, V, KC, HF, VC>::
PutIfExists(const TKey& key, const TValue& newValue, TSearchHint* hint) {
    return Put(key, newValue, TPutCondition(TPutCondition::IF_EXISTS), hint);
}

template <class K, class V, class KC, class HF, class VC>
bool TLFHashTable<K, V, KC, HF, VC>::Delete(const TKey& key) {
    return Put(key, ValueNone(), TPutCondition(TPutCondition::IF_EXISTS));
}

template <class K, class V, class KC, class HF, class VC>
bool TLFHashTable<K, V, KC, HF, VC>::DeleteIfMatch(const TKey& key, const TValue& oldValue) {
    return Put(key, ValueNone(), TPutCondition(TPutCondition::IF_MATCHES, oldValue)); 
}

template <class K, class V, class KC, class HF, class VC>
void TLFHashTable<K, V, KC, HF, VC>::StartGuarding() {
    Guard = NLFHT::TThreadGuardTable::ForTable(static_cast<TLFHashTableBase*>(this));

    while (true) {
        AtomicBase CurrentTableNumber = TableNumber;
        Guard->GuardTable(CurrentTableNumber);
        AtomicBarrier();
        if (TableNumber == CurrentTableNumber) {
            // Atomic operation means memory barrier.
            // Now we are sure, that no thread can delete current Head.
            return;
        }
    }
}

template <class K, class V, class KC, class HF, class VC>
void TLFHashTable<K, V, KC, HF, VC>::StopGuarding() {
    // See comments in StartGuarding.
    if (!Guard)
        Guard = GuardManager.AcquireGuard((size_t)&Guard);

    Guard->StopGuarding();
}

template <class K, class V, class KC, class HF, class VC>
void TLFHashTable<K, V, KC, HF, VC>::TryToDelete() {
    TTable* toDel = ToDelete;
    if (!toDel) 
        return;
    TTable* oldHead = Head;
    AtomicBase firstGuardedTable = GuardManager.GetFirstGuardedTable();

    // if the following is true, it means that no thread works
    // with the tables to ToDelete list
    if (TableToDeleteNumber < firstGuardedTable)
        if (AtomicCas(&ToDelete, (TTable*)0, toDel)) {
            if (Head == oldHead) {
                while (toDel) {
                    TTable* nextToDel = toDel->NextToDelete;
                    delete toDel;
                    toDel = nextToDel;
                }
            }
            else {
                // This is handling of possible ABA problem.
                // If some other table was removed from list,
                // successfull CAS doesn't guarantee that we have 
                // the same table as before. We put all the elements back
                // to the ToDelete list.
                TTable* head = toDel; 
                TTable* tail = head;
                while (tail->Next) {
                    tail = tail->Next;
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

template <class K, class V, class KC, class HF, class VC>
typename TLFHashTable<K, V, KC, HF, VC>::TConstIterator
TLFHashTable<K, V, KC, HF, VC>::Begin() const {
    TConstIterator begin(Head, -1);
    ++begin;
    return begin;
}

// JUST TO DEBUG

template <class K, class V, class KC, class HF, class VC>
void TLFHashTable<K, V, KC, HF, VC>::Print(std::ostream& ostr) {
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

    ostr << buf.str() << '\n';
}

template <class K, class V, class KC, class HF, class VC>
size_t TLFHashTable<K, V, KC, HF, VC>::Size() const {
    size_t result = 0;
    TConstIterator it = Begin();
    while (it.IsValid()) {
        ++result;
        ++it;
    }
    return result;
}

template <class K, class V, class KC, class HF, class VC>
void TLFHashTable<K, V, KC, HF, VC>::Trace(std::ostream& ostr, const char* format, ...) {
    char buf1[10000];
    sprintf(buf1, "Thread %zd: ", (size_t)&errno);

    char buf2[10000];
    va_list args;
    va_start(args, format);
    vsprintf(buf2, format, args);
    va_end(args);

    ostr << buf1 << buf2;
}
