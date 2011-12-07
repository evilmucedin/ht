#pragma once

#include <cerrno>
#include <cstdlib>
#include <cmath>

#include <string>
#include <vector>
#include <iostream>
#include <strstream>

#include "atomic.h"

#if defined __GNUC__
    #define THREAD_LOCAL __thread
#elif defined _MSC_VER
    #define THREAD_LOCAL __declspec(thread)
#else
    #error do not know how to make thread-local variables at this platform
#endif

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
class LFHashTable;

namespace NLFHT {
    const size_t CACHE_LINE_SIZE = 64;

    typedef volatile char State;

    static bool StateCas(State* state, State newValue, State oldValue) {
        return __sync_bool_compare_and_swap(state, newValue, oldValue);  
    } 
    
    static const State NORMAL = 0;
    static const State COPYING = 1;
    static const State COPIED = 2;

    template<Atomic RK1, Atomic RK2, Atomic RV>
    struct Entry {
        static const AtomicBase NO_KEY = RK1;
        static const AtomicBase TOMBSTONE = RK2;
        static const AtomicBase NO_VALUE = RV;

        Atomic m_key;
        Atomic m_value;
        State m_state;

        Entry() :
            m_key(NO_KEY),
            m_value(NO_VALUE),
            m_state(NORMAL)
        {
        }

        // for debug purpose
        static std::string KeyToString(Atomic key) {
            if (key == NO_KEY)
                return "NO_VALUE";
            if (key == TOMBSTONE)
                return "TOMBSTONE";
            return (const char*)(key);
        }

        static std::string ValueToString(Atomic value) {
            if (value == NO_VALUE)
                return "NO_VALUE";
            return (const char*)(value);
        }

        static std::string StateToString(State state) {
            if (state == COPYING)
                return "COPYING";
            if (state == COPIED)
                return "COPIED";
            return "NORMAL";
        }
    };

    template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
    class ConstIterator;

    template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
    class Table {
    private:
        typedef E TEqual;
        typedef H THash;

        typedef Entry<RK1, RK2, RV> EntryT;
        typedef Table<H, E, RK1, RK2, RV> TableT;
        typedef LFHashTable<H, E, RK1, RK2, RV> LFHashTableT;
        typedef ConstIterator<H, E, RK1, RK2, RV> ConstIteratorT;
        typedef typename LFHashTableT::PutCondition PutCondition;

        friend class LFHashTable<H, E, RK1, RK2, RV>;
        friend class ConstIterator<H, E, RK1, RK2, RV>;

        size_t m_size;
        size_t m_maxProbeCnt;
        volatile bool m_isFullFlag;
        
        Atomic m_copiedCnt;
        size_t m_copyTaskSize;

        std::vector<EntryT> m_data;

        LFHashTableT* m_parent;
        TableT *volatile m_next;
        TableT *volatile m_nextToDelete;

        SpinLock Lock;

    public:

        enum EResult {
            FULL_TABLE,
            SUCCEEDED, 
            FAILED
        };

    public:      
        Table(LFHashTableT* parent, size_t size)
            : m_size(size)
            , m_isFullFlag(false)
            , m_copiedCnt(0)
            , m_copyTaskSize(0)
            , m_parent(parent)
            , m_next(0)
            , m_nextToDelete(0)
        {
            VERIFY((m_size & (m_size - 1)) == 0, "Size must be power of two\n");

            m_data.resize(m_size);
            m_maxProbeCnt = (size_t)(1.5 * log((double)m_size));
        }

        bool IsFull() {
            return m_isFullFlag;
        }

        TableT* GetNext() {
            return m_next;
        }

        void CreateNext();

        EntryT* LookUp(AtomicBase key, size_t hash, AtomicBase& foundKey);
        void Copy(EntryT* entry);
        
        EResult Put(AtomicBase key, AtomicBase value, PutCondition cond);
        EResult Delete(AtomicBase key);

        // for debug purpose
        void Print(std::ostream& ostr);

    private:    
        void PrepareToDelete();    
        void DoCopyTask();

        // for debug purpose
        void Trace(std::ostream& ostr, const char* format, ...);
    };

    // NOT thread-safe iterator, use for moments, when only one thread works with table
    template<class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
    class ConstIterator {
    private:
        friend class LFHashTable<H, E, RK1, RK2, RV>;

        typedef Table<H, E, RK1, RK2, RV> TableT;
        typedef Entry<RK1, RK2, RV> EntryT;

        TableT* m_table;
        size_t m_index;
    public:
        typedef LFHashTable<H, E, RK1, RK2, RV> LFHashTableT;
        typedef ConstIterator<H, E, RK1, RK2, RV> ConstIteratorT;

        AtomicBase Key()
        {
            return m_table->Data[m_index].Key;
        }
        AtomicBase Value()
        {
            return m_table->Data[m_index].Value;
        }
        ConstIteratorT& operator++(int)
        {
            NextEntry();
            return *this;
        }
        ConstIteratorT& operator++()
        {
            NextEntry();
            return *this;
        }

        bool IsValid()
        {
            return m_table;
        }
    protected:
        ConstIterator(const ConstIterator& it)
            : m_table(it.Table)
            , m_index(it.Index)
        {
        }
        void NextEntry();    
    private:
        ConstIterator(TableT* table, size_t index)
            : m_table(table)
            , m_index(index)
        {
        }
    };

    class Guard {
    private:
        friend class GuardManager;

        static const AtomicBase NO_OWNER;
        static const AtomicBase NO_TABLE;

        Guard* m_next;
        Atomic m_owner;

        size_t m_guardedTable;
        // to exclude probability, that data from different
        // tables are in the same cache line
        char m_padding[CACHE_LINE_SIZE];

    public:
        TGuard() :
            m_next(0),
            m_owner(NO_OWNER),
            m_guardedTable(NO_TABLE),
            m_copyWrites(0),
            m_putWrites(0)
        {
        }

        void Release() {
            m_owner = NO_OWNER;
        }

        void GuardTable(TAtomicBase tableNumber) {
            m_guardedTable = tableNumber;
        }

        void StopGuarding() {
            m_guardedTable = NO_TABLE;
        }

    public:    
        size_t m_copyWrites;
        size_t m_putWrites;
    };

    class GuardManager {
    private:
        Guard *volatile Head;
    public:
        GuardManager() :
            Head(0)
        {
        }

        TGuard* AcquireGuard(size_t owner) {
            for (TGuard* current = Head; current; current = current->Next)
                if (current->Owner == TGuard::NO_OWNER)
                    if (AtomicCas(&current->Owner, owner, TGuard::NO_OWNER))
                        return current;
            return CreateGuard(owner);
        }

        size_t GetFirstGuardedTable() {
            size_t result = TGuard::NO_TABLE;
            for (TGuard* current = Head; current; current = current->Next)
                if (current->Owner != TGuard::NO_OWNER)
                    result = Min(result, current->GuardedTable);
            return result;
        }

        size_t TotalCopyWrites() {
            size_t result = 0;
            for (TGuard* current = Head; current; current = current->Next)
                result += current->CopyWrites;
            return result;
        }

        size_t TotalPutWrites() {
            size_t result = 0;
            for (TGuard* current = Head; current; current = current->Next)
                result += current->PutWrites;
            return result;
        }

        void ResetCounters() {
            for (TGuard* current = Head; current; current = current->Next)
                current->CopyWrites = current->PutWrites = 0;
        }

    private:
        TGuard* CreateGuard(TAtomicBase owner) {
            TGuard* guard = new TGuard;
            guard->Owner = owner;
            while (true) {
                guard->Next = Head;
                if (AtomicCas(&Head, guard, guard->Next))
                   break;
            } 
            return guard;
        }
    };

};

template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
class TLFHashTable {
private:    
    typedef H THash;
    typedef E TEqual;

    THash Hash;
    TEqual Equal;

    typedef NLFHT::TTable<H, E, RK1, RK2, RV> TTable;
    friend class NLFHT::TTable<H, E, RK1, RK2, RV>;

    TAtomic TableNumber;
    TAtomic TableToDeleteNumber;

    static THREAD_LOCAL NLFHT::TGuard* Guard;
    NLFHT::TGuardManager GuardManager;

    TTable *volatile Head;
    TTable *volatile ToDelete;

public:
    typedef NLFHT::TEntry<RK1, RK2, RV> TEntry;
    typedef NLFHT::TConstIterator<H, E, RK1, RK2, RV> TConstIterator; 

    struct TPutCondition {
        enum EWhenToPut {
            ALWAYS,
            IF_ABSENT,
            IF_EXISTS,
            IF_MATCHES
        };

        EWhenToPut When;
        TAtomicBase Value;

        TPutCondition(EWhenToPut when = ALWAYS, TAtomicBase value = TEntry::NO_VALUE) :
            When(when),
            Value(value)
        {
        } 

        std::string ToString() {
            std::strstream tmp;
            if (When == ALWAYS)
                tmp << "ALWAYS";
            else if (When == IF_ABSENT)
                tmp << "IF_ABSENT";
            else if (When == IF_EXISTS)
                tmp << "IF_EXISTS";
            else 
                tmp << "IF_MATCHES";
            tmp << " with " << TEntry::ValueToString(Value);
            return tmp.str();
        }
    };

    double Density;

public:
    TLFHashTable(double density, size_t initialSize, const THash& hash = THash(), const TEqual& equal = TEqual());

    TAtomicBase Get(TAtomic key);
    // returns true if condition was matched
    bool Put(TAtomic key, TAtomic value, TPutCondition condition = TPutCondition());
    bool PutIfMatch(TAtomicBase key, TAtomicBase oldValue, TAtomicBase newValue);
    bool PutIfAbsent(TAtomicBase key, TAtomicBase value);
    bool PutIfExists(TAtomicBase key, TAtomicBase value); 
    // returns true if key was really deleted
    bool Delete(TAtomic key);

    TConstIterator Begin() const;

    void Print(std::ofstream& ostr);
    void Trace(std::ofstream& ostr, const char* format, ...);

private:
    void StopGuarding(); 
    void StartGuarding();
    void TryToDelete();        
};

namespace NLFHT {
    template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
    TEntry<RK1, RK2, RV>* TTable <H, E, RK1, RK2, RV>::LookUp(TAtomic key, size_t hash, TAtomicBase& foundKey) {
        YASSERT(key != TEntryT::NO_KEY);
#ifdef TRACE
        Trace(Cerr, "LookUp for key \"%s\"\n", ~TEntryT::KeyToString(key));
#endif

        size_t i = hash & (Size - 1);
        size_t probesCnt = 0; 

#ifdef TRACE
        Trace(Cerr, "Start from entry %zd\n", i);
#endif
        foundKey = TEntryT::NO_KEY;
        do {
            TAtomicBase currentKey = Data[i].Key;
            if (currentKey != TEntryT::TOMBSTONE) {
                if (currentKey == TEntryT::NO_KEY) {
#ifdef TRACE
                    Trace(Cerr, "Found empty entry %zd\n", i);
#endif
                    return &Data[i];
                }
                if (Parent->Equal(currentKey, key)) {
#ifdef TRACE
                    Trace(Cerr, "Found key\n");
#endif            
                    foundKey = key;
                    return &Data[i]; 
                }
            }

            i++;
            probesCnt++;
            i &= (Size - 1);
        }
        while (probesCnt < MaxProbeCnt);

#ifdef TRACE
        Trace(Cerr, "No empty entries in table\n");
#endif    
        // Variable value is changed once, thus
        // we have only one cache fault for thread.
        // Better then IsFullFlag = true
        if (!IsFullFlag)
            IsFullFlag = true;

        return 0;
    }

    template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
    void TTable<H, E, RK1, RK2, RV>::CreateNext() {
        Lock.Acquire();
        if (Next) {
            Lock.Release();
            return;
        }
#ifdef TRACE
        Trace(Cerr, "CreateNext\n");
#endif

        size_t tcw = Parent->GuardManager.TotalCopyWrites();
        size_t tpw = Parent->GuardManager.TotalPutWrites();
        size_t nextSize = tcw > tpw ? 2 * Size : Size;
        Next = new TTableT(Parent, nextSize);
#ifdef TRACE
        Trace(Cerr, "Table done\n");
#endif
        CopyTaskSize = 2 * (Size / (size_t)(0.3 * Next->Size + 1));
        Parent->GuardManager.ResetCounters();
#ifdef TRACE_MEM
        Trace(Cerr, "TCW = %zd, TPW = %zd\n", tcw, tpw);
        Trace(Cerr, "New table of size %d\n", Next->Size);
#endif

        Lock.Release();
    }

    template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
    void TTable<H, E, RK1, RK2, RV>::Copy(TEntry<RK1, RK2, RV>* entry) {
        TAtomicBase entryKey = entry->Key;
        if (entryKey == TEntryT::NO_KEY || entryKey == TEntryT::TOMBSTONE) {
            entry->State = COPIED;
            return;
        }
#ifdef TRACE
        Trace(Cerr, "Copy \"%s\"\n", entry->Key);
#endif

        if (entry->State == NORMAL)
            StateCas(&entry->State, COPYING, NORMAL);
        // by now entry is locked for modifications
        // that means, that each thread that succeeds with operation on it
        // will have to repeat this operation in the next table

        // remember the value to copy to the next table
        TAtomicBase entryValue = entry->Value;

        // we don't need to copy values that are not set yet
        if (entryValue == TEntryT::NO_VALUE) {
            entry->State = COPIED;
#ifdef TRACE
            Trace(Cerr, "Don't need to copy empty entry\n");
#endif
            return;
        }

        size_t hashValue = Parent->Hash(entryKey); 

        TTableT* current = this;
        while (entry->State == COPYING) {
            if (!current->Next)
                current->CreateNext();
            TTableT* target = current->Next;
            TAtomicBase foundKey;
            TEntryT* dest = target->LookUp(entryKey, hashValue, foundKey);

            // can't insert anything in full table
            if (target->IsFull() || dest == 0) {
                current = target;
                continue;
            }
            // try to get entry for current key
            if (foundKey == TEntryT::NO_KEY) { 
                if (!AtomicCas(&dest->Key, entryKey, TEntryT::NO_KEY))
                    // lost race to install new key
                    continue;
            }
            if (dest->Value == TEntryT::NO_VALUE)
                if (AtomicCas(&dest->Value, entryValue, TEntryT::NO_VALUE))
                    Parent->Guard->CopyWrites++;
                // else we already have some real value in the next table,
                // it means copying is done
            if (dest->State != NORMAL)
                continue;

            entry->State = COPIED;
        }
    }   

    template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
    typename TTable<H, E, RK1, RK2, RV>::EResult TTable<H, E, RK1, RK2, RV>::Put(TAtomic key, TAtomic value, TPutCondition cond) {
#ifdef TRACE
        Trace(Cerr, "Put key \"%s\" and value \"%s\" under condition %s..\n",
                          key, value, ~cond.ToString());
#endif

        size_t hashValue = Parent->Hash(key);

        TEntryT* entry = 0;
        while (!entry)
        {
            TAtomicBase foundKey;
            entry = LookUp(key, hashValue, foundKey);
            if (!entry) 
                return FULL_TABLE;
            if (IsFull()) {
                Copy(entry);
                return FULL_TABLE;
            }
#ifdef TRACE
            Trace(Cerr, "Consider entry %d\n", entry - &Data[0]);
#endif
            if (foundKey == TEntryT::NO_KEY) { 
                if (cond.When == TPutCondition::IF_EXISTS ||
                    cond.When == TPutCondition::IF_MATCHES)
                    return FAILED;
                if (!AtomicCas(&entry->Key, key, TEntryT::NO_KEY)) {
#ifdef TRACE
                    Trace(Cerr, "Lost race for instaling key\n");
#endif
                    entry = 0;
                }
            }
        }

#ifdef TRACE
        Trace(Cerr, "Got entry %d\n", entry - &Data[0]);
#endif
        while (true) {
            // entry can not be in COPYING if table is not full, 
            // but there can be small possible overhead (see 4)
            if (entry->State == COPYING)
                Copy(entry);
            if (entry->State == COPIED)
                return FAILED;

            if (entry->Value == TEntryT::NO_VALUE) {
                if (cond.When == TPutCondition::IF_EXISTS)
                    return FAILED;
                if (AtomicCas(&entry->Value, value, TEntryT::NO_VALUE)
                    && entry->State == NORMAL) { // check that entry wasn't copied during assignment 
                    Parent->Guard->PutWrites++;
                    return SUCCEEDED; 
                }
            }
            if (cond.When == TPutCondition::IF_ABSENT)
                return FAILED;
            
            // we can set this value no matter what state is entry in
            TAtomic oldValue = entry->Value;
            if (cond.When == TPutCondition::IF_MATCHES && oldValue != cond.Value)
                return FAILED;
            if (AtomicCas(&entry->Value, value, oldValue)
                    && entry->State == NORMAL) {  
                return SUCCEEDED;
            }
        }
    }

    template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
    typename TTable<H, E, RK1, RK2, RV>::EResult TTable<H, E, RK1, RK2, RV>::Delete(TAtomic key) {
#ifdef TRACE
        Trace(Cerr, "Delete \"%s\"\n", key);
#endif

        size_t hashValue = Parent->Hash(key);
        TAtomicBase foundKey;
        TEntryT* entry = LookUp(key, hashValue, foundKey);

        //  if table is full we can't say anything
        if (!entry) return FULL_TABLE;

        while (true) {
            if (entry->State == COPYING)
                Copy(entry);
            if (entry->State == COPIED)
                return FULL_TABLE;

            if (foundKey == TEntryT::NO_KEY) {
                // if table is full, entry can be in the next table
                if (IsFull())
                    return FULL_TABLE;
                // otherwise we can suppose there is no such key in table
                // but nothing was really deleted
                return FAILED;
            }            

            entry->Key = TEntryT::TOMBSTONE;
            if (entry->State == NORMAL)
                return SUCCEEDED;
        }
    }

    template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
    void TTable<H, E, RK1, RK2, RV>::DoCopyTask() {
        if ((size_t)CopiedCnt < Size) {
            size_t finish = AtomicAdd(CopiedCnt, CopyTaskSize);
            size_t start = finish - CopyTaskSize;
            if (start < Size) {
                finish = std::min(Size, finish);
#ifdef TRACE
                Trace(Cerr, "Copy from %d to %d\n", start, finish); 
#endif        
                for (size_t i = start; i < finish; i++)
                    Copy(&Data[i]);   
            }
        }

        if ((size_t)CopiedCnt >= Size)
            PrepareToDelete();
    }

    template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
    void TTable<H, E, RK1, RK2, RV>::PrepareToDelete() {
#ifdef TRACE
        Trace(Cerr, "PrepareToDelete\n");
#endif
        TAtomicBase currentTableNumber = Parent->TableNumber;
        if (Parent->Head == this && AtomicCas(&Parent->Head, Next, this)) {
            // deleted table from main list
            // now it's only thread that has pointer to it
            AtomicIncrement(Parent->TableNumber);
            Parent->TableToDeleteNumber = currentTableNumber;
            while (true) {
                TTable* toDelete = Parent->ToDelete;
                NextToDelete = toDelete;
                if (AtomicCas(&Parent->ToDelete, this, toDelete)) {
#ifdef TRACE_MEM
                    Trace(Cerr, "Scheduled to delete table %zd\n", (size_t)this);
#endif
                    break;
                }
            }
        }
    }

    template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
    void TTable<H, E, RK1, RK2, RV>::Print(TOutputStream& ostr) {
        TStringStream buf;

        buf << "Table at " << (size_t)this << ":\n";

        buf << "Size: " << Size << '\n'
            << "CopiedCnt: " << CopiedCnt << '\n'
            << "CopyTaskSize: " << CopyTaskSize << '\n';

        for (size_t i = 0; i < Size; i++) 
            buf << "Entry " << i << ": "
                << "(" << TEntryT::KeyToString(Data[i].Key)
                << "; " << TEntryT::ValueToString(Data[i].Value)  
                << "; " << TEntryT::StateToString(Data[i].State)
                << ")\n";

        ostr << buf.Str();
    }
   
    template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
    void TTable<H, E, RK1, RK2, RV>::Trace(TOutputStream& ostr, const char* format, ...) {
        std::string buf1;
        sprintf(buf1, "Thread %zd in table %zd: ", (size_t)&errno, (size_t)this);

        std::string buf2;
        va_list args;
        va_start(args, format);
        vsprintf(buf2, format, args);
        va_end(args);

        ostr << buf1 + buf2;
    }

    template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
    void TConstIterator<H, E, RK1, RK2, RV>::NextEntry() {
        Index++;

        while (Table) {
            for (; Index < Table->Size; Index++) {
                // it's rather fast to copy small entry
                const TEntryT& e = Table->Data[Index];
                if (e.Key != TEntryT::NO_KEY && e.Key != TEntryT::TOMBSTONE && e.Value != TEntryT::NO_VALUE && e.State == NORMAL)
                    break;
            }
            if (Index < Table->Size)
                break;
            Index = 0;
            Table = Table->Next;
        }
    }
}

template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
THREAD_LOCAL NLFHT::TGuard* TLFHashTable<H, E, RK1, RK2, RV>::Guard; 

template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
TLFHashTable<H, E, RK1, RK2, RV>::TLFHashTable(double density, size_t initialSize, const THash& hash, const TEqual& equal) :
    Hash(hash),
    Equal(equal),
    TableNumber(0),
    TableToDeleteNumber(Max<TAtomicBase>()),
    ToDelete(0),
    Density(density)
{
    VERIFY((initialSize & (initialSize - 1)) == 0, "Table size must be power of two");
    Head = new TTable(this, initialSize); // size must be power of 2
#ifdef TRACE
    Trace(Cerr, "TLFHashTable created\n");
#endif
}

template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
TAtomicBase TLFHashTable<H, E, RK1, RK2, RV>::Get(TAtomic key) {
    YASSERT(key != TEntry::NO_KEY);

    StartGuarding();

#ifdef TRACE
    Trace(Cerr, "Get \"%s\"\n", key);
#endif

    size_t hashValue = Hash(key);

    TAtomicBase returnValue = TEntry::NO_VALUE;
    for (TTable* cur = Head; cur; cur = cur->GetNext()) {
        TAtomicBase foundKey;
        TEntry* entry = cur->LookUp(key, hashValue, foundKey);

        if (foundKey == TEntry::NO_KEY) {
            // we can't insert in full table, if table is full we should continue search
            // but if it's not full, NO_KEY means we found an empty entry
            if (!cur->IsFull()) 
                break;
        }
        else {
            TAtomicBase value = entry->Value;
            if (entry->State == NLFHT::COPYING)
                cur->Copy(entry); 
            if (entry->State != NLFHT::COPIED) {
                returnValue = value;
                break;
            }
        }

        // if state is SHADOW, we should either find 
    }

    StopGuarding();
    return returnValue; 
}

// returns true if new key appeared in a table
template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
bool TLFHashTable<H, E, RK1, RK2, RV>::Put(TAtomic key, TAtomic value, TPutCondition cond) {
    YASSERT(key != TEntry::NO_KEY);
    YASSERT(value != TEntry::NO_VALUE);

    if (cond.When == TPutCondition::IF_MATCHES && cond.Value == TEntry::NO_VALUE)
        cond.When = TPutCondition::IF_ABSENT;

    StartGuarding();

#ifdef TRACE
    Trace(Cerr, "Put key \"%s\" and value \"%s\" under condition %s..\n",
            TEntry::KeyToString(key).c_str(), TEntry::ValueToString(value).c_str(),
            ~cond.ToString());
#endif
    TTable* cur = Head;
    typename TTable::EResult result;
    while (true) {
        if ((result = cur->Put(key, value, cond)) != TTable::FULL_TABLE)
            break;
        if (!cur->GetNext()) {
#ifdef TRACE
            Trace(Cerr, "Create next table to put new key\n");
#endif            
            cur->CreateNext();
        }
        cur->DoCopyTask();
        cur = cur->GetNext();
    }
    
    StopGuarding();
    TryToDelete();

    return result == TTable::SUCCEEDED;
}

template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
bool TLFHashTable<H, E, RK1, RK2, RV>::PutIfMatch(TAtomicBase key, 
        TAtomicBase oldValue, TAtomicBase newValue) {
    return Put(key, newValue, TPutCondition(TPutCondition::IF_MATCHES, oldValue));
}

template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
bool TLFHashTable<H, E, RK1, RK2, RV>::PutIfAbsent(TAtomicBase key, 
        TAtomicBase newValue) {
    return Put(key, newValue, TPutCondition(TPutCondition::IF_ABSENT));
}

template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
bool TLFHashTable<H, E, RK1, RK2, RV>::PutIfExists(TAtomicBase key, 
        TAtomicBase newValue) {
    return Put(key, newValue, TPutCondition(TPutCondition::IF_EXISTS));
}

template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
bool TLFHashTable<H, E, RK1, RK2, RV>::Delete(TAtomic key) {
    YASSERT(key != TEntry::NO_KEY);

    StartGuarding();

#ifdef TRACE
    Trace(Cerr, "Delete \"%s\"\n", key);
#endif
    TTable* cur = Head;
    typename TTable::EResult result;
    while (true) {
        if ((result = cur->Delete(key)) != TTable::FULL_TABLE)
            break;
        if (!cur->GetNext())
            break;
        cur = cur->GetNext();
    }

    StopGuarding();
    TryToDelete();

    return result == TTable::SUCCEEDED;
}

template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
void TLFHashTable<H, E, RK1, RK2, RV>::StartGuarding() {
    // Guard is thread local storage. &Guard is unique for each thread.
    // Thus, it can be used as identifier.
    if (!Guard)
        Guard = GuardManager.AcquireGuard((size_t)&Guard);

    while (true) {
        TAtomicBase CurrentTableNumber = TableNumber;
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

template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
void TLFHashTable<H, E, RK1, RK2, RV>::StopGuarding() {
    // See comments in StartGuarding.
    if (!Guard)
        Guard = GuardManager.AcquireGuard((size_t)&Guard);

    Guard->StopGuarding();
#ifdef TRACE
    Trace(Cerr, "Stopped guarding\n");
#endif
}

template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
void TLFHashTable<H, E, RK1, RK2, RV>::TryToDelete() {
#ifdef TRACE
    Trace(Cerr, "TryToDelete\n");
#endif
    TTable* toDel = ToDelete;
    if (!toDel) 
        return;
    TTable* oldHead = Head;
    TAtomicBase firstGuardedTable = GuardManager.GetFirstGuardedTable();

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

template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
void TLFHashTable<H, E, RK1, RK2, RV>::Print(TOutputStream& ostr) {
    TStringStream buf;
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

    ostr << buf.Str() << '\n';
}


template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
void TLFHashTable<H, E, RK1, RK2, RV>::Trace(TOutputStream& ostr, const char* format, ...) {
    std::string buf1;
    sprintf(buf1, "Thread %zd: ", (size_t)&errno);

    std::string buf2;
    va_list args;
    va_start(args, format);
    vsprintf(buf2, format, args);
    va_end(args);

    ostr << buf1 + buf2;
}

template <class H, class E, TAtomic RK1, TAtomic RK2, TAtomic RV>
NLFHT::TConstIterator<H, E, RK1, RK2, RV> TLFHashTable<H, E, RK1, RK2, RV>::Begin() const {
    TConstIterator begin(Head, -1);
    begin++;
    return begin;
}

