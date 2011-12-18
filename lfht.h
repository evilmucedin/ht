#pragma once

#include "table.h"
#include "guards.h"

#include <cstdlib>
#include <cmath>

#include <limits>

class TLFHRegistration {
public:    
    TLFHRegistration() {
        NLFHT::TThreadGuardTable::InitializePerThread();
    }
    ~TLFHRegistration() {
        NLFHT::TThreadGuardTable::FinalizePerThread();
    }
};

template <class K, class V>
class TLFHashTable {
private:    
    typedef NLFHT::TKeyTraits<K> TKeyTraits;       
    typedef NLFHT::TValueTraits<V> TValueTraits;

    typedef typename NLFHT::TKeyTraits<K>::TKey TKey;
    typedef typename NLFHT::TValueTraits<V>::TValue TValue;

    typedef typename NLFHT::TKeyTraits<K>::THashFunc THashFunc; 
    typedef typename NLFHT::TKeysAreEqual<K> TKeysAreEqual;
    typedef typename NLFHT::TValuesAreEqual<V> TValuesAreEqual;

    typedef NLFHT::TTable<K, V> TTable;
    friend class NLFHT::TTable<K, V>;

    typedef NLFHT::TGuard TGuard;
    typedef NLFHT::TGuardManager TGuardManager;

    // functors    
    THashFunc Hash;
    TKeysAreEqual KeysAreEqual;
    TValuesAreEqual ValuesAreEqual;
    TKeyTraits KeyTraits;
    TValueTraits ValueTraits;

    // number of head table (in fact it can be less or equal)
    Atomic TableNumber;
    // number of head table of ToDelete list (in fact it can be greater or equal) 
    Atomic TableToDeleteNumber;

    // guarded related stuff
    static NLFHT_THREAD_LOCAL TGuard* Guard;
    TGuardManager GuardManager;

    // whole table structure    
    TTable *volatile Head;
    TTable *volatile ToDelete;

    double Density;

public:
    typedef NLFHT::TEntry<K, V> TEntry;
    typedef NLFHT::TConstIterator<K, V> TConstIterator;

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
            TSearchHint() :
                Table(0)
            {
            }
        private:
            friend class TLFHashTable<K, V>;
            friend class NLFHT::TTable<K, V>;

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
    TLFHashTable(size_t initialSize = 1, double density = .5,
                 const THashFunc& hash = THashFunc(),
                 const TKeysAreEqual& keysAreEqual = TKeysAreEqual(),
                 const TValuesAreEqual& valuesAreEqual = TValuesAreEqual()); 
    ~TLFHashTable();

    // return TOMBSTONE value if there is no such key
    TValue Get(const TKey& key, TSearchHint* hint = 0);
    // TOMBSTONE value getter to compare with
    static TValue Tombstone() { return ValueDeleted(); } 

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

    // iterator related stuff
    TConstIterator Begin() const;

    // JUST TO DEBUG
    void Print(std::ostream& ostr);
    void PrintStatistics(std::ostream& str) {
        GuardManager.PrintStatistics(str);
    }

    TKeyTraits& GetKeyTraits() {
        return KeyTraits;
    }
    TValueTraits& GetValueTraits() {
        return ValueTraits;
    }

    size_t Size();

private:
    // thread-safefy and lock-free memory reclamation is done here
    void StopGuarding(); 
    void StartGuarding();
    void TryToDelete();        

    // traits wrappers
    static TKey KeyNone() {
        return NLFHT::TKeyTraits<K>::None();
    }
    void UnRefKey(const TKey& key, size_t cnt = 1) {
        KeyTraits.UnRef(key, cnt);
    }

    static TValue ValueNone() {
        return NLFHT::TValueTraits<V>::None();
    }
    static TValue ValueBaby() {
        return NLFHT::TValueTraits<V>::Baby();
    }
    static TValue ValueDeleted() {
        return NLFHT::TValueTraits<V>::Deleted();
    }
    void UnRefValue(const TValue& value, size_t cnt = 1) {
        ValueTraits.UnRef(value, cnt);
    }

    static std::string KeyToString(const TKey& key) {
        return NLFHT::KeyToString<K>(key);
    }
    static std::string ValueToString(const TValue& value) {
        return NLFHT::ValueToString<V>(value);
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

template <class K, class V>
NLFHT_THREAD_LOCAL NLFHT::TGuard* TLFHashTable<K, V>::Guard = 0;

template <class K, class V>
TLFHashTable<K, V>::TLFHashTable(size_t initialSize, double density,
                                 const THashFunc& hash,
                                 const TKeysAreEqual& keysAreEqual, 
                                 const TValuesAreEqual& valuesAreEqual)
    : Hash(hash)
    , KeysAreEqual(keysAreEqual)
    , ValuesAreEqual(valuesAreEqual)
    , TableNumber(0)
    , TableToDeleteNumber(std::numeric_limits<AtomicBase>::max())
    , ToDelete(0)
    , Density(density)
{
    assert(Density < 1.);
    Head = new TTable(this, initialSize);
    Guard = 0;
#ifdef TRACE
    Trace(Cerr, "TLFHashTable created\n");
#endif
}

template <class K, class V>
TLFHashTable<K, V>::~TLFHashTable() {
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

template <class K, class V>
typename TLFHashTable<K, V>::TValue TLFHashTable<K, V>::Get(const TKey& key, TSearchHint* hint) {
    assert(!KeysAreEqual(key, KeyNone()));
#ifdef TRACE
    Trace(Cerr, "TLFHashTable.Get(%s)\n", ~KeyToString(key));
#endif

    StartGuarding();
    OnGet();

    TTable* startTable = Head;

#ifdef TRACE
    Trace(Cerr, "Get \"%s\"\n", ~KeyToString(key));
#endif

    size_t hashValue = Hash(key);
    TValue returnValue = ValueDeleted();
    for (TTable* cur = startTable; 
         cur && !cur->Get(key, hashValue, returnValue, hint);
         cur = cur->GetNext());

    StopGuarding();
#ifdef TRACE
    Trace(Cerr, "Get returns %s\n", ~ValueToString(returnValue));
#endif
    return returnValue; 
}

// returns true if new key appeared in a table
template <class K, class V>
bool TLFHashTable<K, V>::Put(const TKey& key, const TValue& value, 
                             TPutCondition cond, TSearchHint* hint) {
    assert(!KeysAreEqual(key, KeyNone()));
#ifdef TRACE
    Trace(Cerr, "TLFHashTable.Put key \"%s\" and value \"%s\" under condition %s..\n",
            ~KeyToString(key), ~ValueToString(value),
            ~cond.ToString());
#endif

    StartGuarding();
    OnPut();

    typename TTable::EResult result;
    bool keyInstalled;

    TTable* cur = Head;
    size_t cnt = 0;
    while (true) {
        if (++cnt >= 100000)
            VERIFY(false, "Too long table list");
        if ((result = cur->Put(key, value, cond, keyInstalled)) != TTable::FULL_TABLE)
            break;
        if (!cur->GetNext()) {
#ifdef TRACE
            Trace(Cerr, "Create next table to put new key");
#endif            
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

#ifdef TRACE
    Trace(Cerr, "%s\n", result == TTable::SUCCEEDED ? "SUCCESS" : "FAIL");
#endif

    return result == TTable::SUCCEEDED;
}

template <class K, class V>
bool TLFHashTable<K, V>::PutIfMatch(const TKey& key, 
                                    const TValue& newValue, const TValue& oldValue, 
                                    TSearchHint* hint) {
    return Put(key, newValue, TPutCondition(TPutCondition::IF_MATCHES, oldValue), hint);
}

template <class K, class V>
bool TLFHashTable<K, V>::PutIfAbsent(const TKey& key, const TValue& value, TSearchHint* hint) {
    return Put(key, value, TPutCondition(TPutCondition::IF_ABSENT, ValueBaby()), hint);
}

template <class K, class V>
bool TLFHashTable<K, V>::PutIfExists(const TKey& key, const TValue& newValue, TSearchHint* hint) {
    return Put(key, newValue, TPutCondition(TPutCondition::IF_EXISTS), hint);
}

template <class K, class V>
bool TLFHashTable<K, V>::Delete(const TKey& key) {
    return Put(key, ValueDeleted(), TPutCondition(TPutCondition::IF_EXISTS));
}

template <class K, class V>
void TLFHashTable<K, V>::StartGuarding() {
    // Guard is thread local storage. &Guard is unique for each thread.
    // Thus, it can be used as identifier.
    TGuard*& guardToSet = NLFHT::TThreadGuardTable::ForTable((void*)this);
    if (!guardToSet)
        guardToSet = GuardManager.AcquireGuard((size_t)&Guard);
    Guard = guardToSet;

    while (true) {
        AtomicBase CurrentTableNumber = TableNumber;
#ifdef TRACE
        Trace(Cerr, "Try to guard table %lld\n", CurrentTableNumber);
#endif
        Guard->GuardTable(CurrentTableNumber);
        AtomicBarrier();
        if (TableNumber == CurrentTableNumber) {
#ifdef TRACE
            Trace(Cerr, "Started guarding\n");
#endif
            // Atomic operation means memory barrier.
            // Now we are sure, that no thread can delete current Head.
            return;
        }
    }
}

template <class K, class V>
void TLFHashTable<K, V>::StopGuarding() {
    // See comments in StartGuarding.
    if (!Guard)
        Guard = GuardManager.AcquireGuard((size_t)&Guard);

    Guard->StopGuarding();
#ifdef TRACE
    Trace(Cerr, "Stopped guarding\n");
#endif
}

template <class K, class V>
void TLFHashTable<K, V>::TryToDelete() {
#ifdef TRACE
    Trace(Cerr, "TryToDelete\n");
#endif
    TTable* toDel = ToDelete;
    if (!toDel) 
        return;
    TTable* oldHead = Head;
    AtomicBase firstGuardedTable = GuardManager.GetFirstGuardedTable();

    // if the following is true, it means that no thread works
    // with the tables to ToDelete list
#ifdef TRACE
    Trace(Cerr, "TableToDeleteNumber %lld, firstGuardedTable %lld\n", TableToDeleteNumber, firstGuardedTable); 
#endif
    if (TableToDeleteNumber < firstGuardedTable)
        if (AtomicCas(&ToDelete, (TTable*)0, toDel)) {
            if (Head == oldHead) {
                while (toDel) {
                    TTable* nextToDel = toDel->NextToDelete;
#ifdef TRACE_MEM
                    Trace(Cerr, "Deleted table %zd of size %zd\n", (size_t)toDel, toDel->Size);
#endif
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
                TTable* tail;
                for (tail = head; tail->Next; tail = tail->Next);

                while (true) {
                    TTable* oldToDelete = ToDelete;
                    tail->NextToDelete = oldToDelete;
                    if (AtomicCas(&ToDelete, head, oldToDelete))
                        break;
                }
#ifdef TRACE_MEM
                Trace(Cerr, "In fear of ABA problem put tables back to list\n");
#endif
            }
        }
}

template <class K, class V>
NLFHT::TConstIterator<K, V> TLFHashTable<K, V>::Begin() const {
    TConstIterator begin(Head, -1);
    begin++;
    return begin;
}

// JUST TO DEBUG

template <class K, class V>
void TLFHashTable<K, V>::Print(std::ostream& ostr) {
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

template <class K, class V>
size_t TLFHashTable<K, V>::Size()
{
    size_t result = 0;
    TConstIterator it = Begin();
    while (it.IsValid())
    {
        ++result;
        ++it;
    }
    return result;
}

template <class K, class V>
void TLFHashTable<K, V>::Trace(std::ostream& ostr, const char* format, ...) {
    char buf1[10000];
    sprintf(buf1, "Thread %zd: ", (size_t)&errno);

    char buf2[10000];
    va_list args;
    va_start(args, format);
    vsprintf(buf2, format, args);
    va_end(args);

    ostr << buf1 << buf2;
}
