#pragma once

#include <cerrno>
#include <cstdlib>
#include <cmath>

#include <string>
#include <vector>
#include <iostream>
#include <sstream>

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
        typedef E Equal;
        typedef H Hash;

        Equal m_equal;
        Hash m_hash;

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

        SpinLock m_lock;

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

        bool IsFull()
        {
            return m_isFullFlag;
        }

        TableT* GetNext()
        {
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
        Guard() :
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

        void GuardTable(AtomicBase tableNumber) {
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
        Guard *volatile m_head;
    public:
        GuardManager()
            : m_head(0)
        {
        }

        Guard* AcquireGuard(size_t owner) {
            for (Guard* current = m_head; current; current = current->m_next)
                if (current->m_owner == Guard::NO_OWNER)
                    if (AtomicCas(&current->m_owner, owner, Guard::NO_OWNER))
                        return current;
            return CreateGuard(owner);
        }

        size_t GetFirstGuardedTable() {
            size_t result = Guard::NO_TABLE;
            for (Guard* current = m_head; current; current = current->m_next)
                if (current->m_owner != Guard::NO_OWNER)
                    result = Min(result, current->m_guardedTable);
            return result;
        }

        size_t TotalCopyWrites() {
            size_t result = 0;
            for (Guard* current = m_head; current; current = current->m_next)
                result += current->m_copyWrites;
            return result;
        }

        size_t TotalPutWrites() {
            size_t result = 0;
            for (Guard* current = m_head; current; current = current->m_next)
                result += current->m_putWrites;
            return result;
        }

        void ResetCounters() {
            for (Guard* current = m_head; current; current = current->m_next)
                current->m_copyWrites = current->m_putWrites = 0;
        }

    private:
        Guard* CreateGuard(AtomicBase owner) {
            Guard* guard = new Guard;
            guard->m_owner = owner;
            while (true) {
                guard->m_next = m_head;
                if (AtomicCas(&m_head, guard, guard->m_next))
                   break;
            } 
            return guard;
        }
    };
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
class LFHashTable {
private:    
    typedef H Hash;
    typedef E Equal;

    Hash m_hash;
    Equal m_equal;

    typedef NLFHT::Table<H, E, RK1, RK2, RV> Table;
    friend class NLFHT::Table<H, E, RK1, RK2, RV>;

    Atomic m_tableNumber;
    Atomic m_tableToDeleteNumber;

    static THREAD_LOCAL NLFHT::Guard* m_guard;
    NLFHT::GuardManager m_guardManager;

    Table *volatile m_head;
    Table *volatile m_toDelete;
    double m_density;
    Atomic m_size;

public:
    typedef NLFHT::Entry<RK1, RK2, RV> Entry;
    typedef NLFHT::ConstIterator<H, E, RK1, RK2, RV> ConstIterator;

    struct PutCondition {
        enum EWhenToPut {
            ALWAYS,
            IF_ABSENT,
            IF_EXISTS,
            IF_MATCHES
        };

        EWhenToPut m_when;
        AtomicBase m_value;

        PutCondition(EWhenToPut when = ALWAYS, AtomicBase value = Entry::NO_VALUE)
            : m_when(when)
            , m_value(value)
        {
        } 

        std::string ToString() {
            std::stringstream tmp;
            if (m_when == ALWAYS)
                tmp << "ALWAYS";
            else if (m_when == IF_ABSENT)
                tmp << "IF_ABSENT";
            else if (m_when == IF_EXISTS)
                tmp << "IF_EXISTS";
            else 
                tmp << "IF_MATCHES";
            tmp << " with " << Entry::ValueToString(m_value);
            return tmp.str();
        }
    };

public:
    LFHashTable(double density, size_t initialSize, const Hash& hash = Hash(), const Equal& equal = Equal());
    LFHashTable();
    LFHashTable(size_t initialSize);

    AtomicBase Get(Atomic key);
    bool Find(Atomic key, Atomic& value);
    // returns true if condition was matched
    bool Put(Atomic key, Atomic value, PutCondition condition = PutCondition());
    bool PutIfMatch(AtomicBase key, AtomicBase oldValue, AtomicBase newValue);
    bool PutIfAbsent(AtomicBase key, AtomicBase value);
    bool PutIfExists(AtomicBase key, AtomicBase value);
    // returns true if key was really deleted
    bool Delete(Atomic key);
    void Clear();
    inline size_t Size() const
    {
        return m_size;
    }

    ConstIterator Begin() const;

    void Print(std::ostream& ostr);
    void Trace(std::ostream& ostr, const char* format, ...);

private:
    void StopGuarding(); 
    void StartGuarding();
    void TryToDelete();        
};

namespace NLFHT {
    template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
    Entry<RK1, RK2, RV>* Table <H, E, RK1, RK2, RV>::LookUp(Atomic key, size_t hash, AtomicBase& foundKey) {
        assert(key != EntryT::NO_KEY);
#ifdef TRACE
        Trace(std::cerr, "LookUp for key \"%s\"\n", ~EntryT::KeyToString(key));
#endif

        size_t i = hash & (m_size - 1);
        size_t probesCnt = 0; 

#ifdef TRACE
        Trace(std::cerr, "Start from entry %zd\n", i);
#endif
        foundKey = EntryT::NO_KEY;
        do {
            AtomicBase currentKey = m_data[i].m_key;
            if (currentKey != EntryT::TOMBSTONE) {
                if (currentKey == EntryT::NO_KEY) {
#ifdef TRACE
                    Trace(std::cerr, "Found empty entry %zd\n", i);
#endif
                    return &m_data[i];
                }
                if (m_parent->m_equal(currentKey, key)) {
#ifdef TRACE
                    Trace(std::cerr, "Found key\n");
#endif            
                    foundKey = key;
                    return &m_data[i];
                }
            }

            ++i;
            ++probesCnt;
            i &= (m_size - 1);
        }
        while (probesCnt < m_maxProbeCnt);

#ifdef TRACE
        Trace(std::cerr, "No empty entries in table\n");
#endif    
        // Variable value is changed once, thus
        // we have only one cache fault for thread.
        // Better then IsFullFlag = true
        if (!m_isFullFlag)
            m_isFullFlag = true;

        return 0;
    }

    template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
    void Table<H, E, RK1, RK2, RV>::CreateNext() {
        m_lock.Acquire();
        if (m_next) {
            m_lock.Release();
            return;
        }
#ifdef TRACE
        Trace(std::cerr, "CreateNext\n");
#endif

        size_t tcw = m_parent->m_guardManager.TotalCopyWrites();
        size_t tpw = m_parent->m_guardManager.TotalPutWrites();
        size_t nextSize = tcw > tpw ? 2 * m_size : m_size;
        m_next = new TableT(m_parent, nextSize);
#ifdef TRACE
        Trace(std::cerr, "Table done\n");
#endif
        m_copyTaskSize = 2 * (m_size / (size_t)(0.3 * m_next->m_size + 1));
        m_parent->m_guardManager.ResetCounters();
#ifdef TRACE_MEM
        Trace(std::cerr, "TCW = %zd, TPW = %zd\n", tcw, tpw);
        Trace(std::cerr, "New table of size %d\n", m_next->m_size);
#endif

        m_lock.Release();
    }

    template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
    void Table<H, E, RK1, RK2, RV>::Copy(Entry<RK1, RK2, RV>* entry) {
        AtomicBase entryKey = entry->m_key;
        if (entryKey == EntryT::NO_KEY || entryKey == EntryT::TOMBSTONE) {
            entry->m_state = COPIED;
            return;
        }
#ifdef TRACE
        Trace(std::cerr, "Copy \"%s\"\n", entry->Key);
#endif

        if (entry->m_state == NORMAL)
            StateCas(&entry->m_state, COPYING, NORMAL);
        // by now entry is locked for modifications
        // that means, that each thread that succeeds with operation on it
        // will have to repeat this operation in the next table

        // remember the value to copy to the next table
        AtomicBase entryValue = entry->m_value;

        // we don't need to copy values that are not set yet
        if (entryValue == EntryT::NO_VALUE) {
            entry->m_state = COPIED;
#ifdef TRACE
            Trace(std::cerr, "Don't need to copy empty entry\n");
#endif
            return;
        }

        size_t hashValue = m_parent->m_hash(entryKey);

        TableT* current = this;
        while (entry->m_state == COPYING) {
            if (!current->m_next)
                current->CreateNext();
            TableT* target = current->m_next;
            AtomicBase foundKey;
            EntryT* dest = target->LookUp(entryKey, hashValue, foundKey);

            // can't insert anything in full table
            if (target->IsFull() || dest == 0) {
                current = target;
                continue;
            }
            // try to get entry for current key
            if (foundKey == EntryT::NO_KEY) {
                if (!AtomicCas(&dest->m_key, entryKey, EntryT::NO_KEY))
                    // lost race to install new key
                    continue;
            }
            if (dest->m_value == EntryT::NO_VALUE)
                if (AtomicCas(&dest->m_value, entryValue, EntryT::NO_VALUE))
                    ++m_parent->m_guard->m_copyWrites;
                // else we already have some real value in the next table,
                // it means copying is done
            if (dest->m_state != NORMAL)
                continue;

            entry->m_state = COPIED;
        }
    }   

    template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
    typename Table<H, E, RK1, RK2, RV>::EResult Table<H, E, RK1, RK2, RV>::Put(Atomic key, Atomic value, PutCondition cond) {
#ifdef TRACE
        Trace(std::cerr, "Put key \"%s\" and value \"%s\" under condition %s..\n",
                          key, value, ~cond.ToString());
#endif

        size_t hashValue = m_parent->m_hash(key);

        EntryT* entry = 0;
        while (!entry)
        {
            AtomicBase foundKey;
            entry = LookUp(key, hashValue, foundKey);
            if (!entry) 
                return FULL_TABLE;
            if (IsFull()) {
                Copy(entry);
                return FULL_TABLE;
            }
#ifdef TRACE
            Trace(std::cerr, "Consider entry %d\n", entry - &Data[0]);
#endif
            if (foundKey == EntryT::NO_KEY) {
                if (cond.m_when == PutCondition::IF_EXISTS ||
                    cond.m_when == PutCondition::IF_MATCHES)
                    return FAILED;
                if (!AtomicCas(&entry->m_key, key, EntryT::NO_KEY)) {
#ifdef TRACE
                    Trace(std::cerr, "Lost race for instaling key\n");
#endif
                    entry = 0;
                }
            }
        }

#ifdef TRACE
        Trace(std::cerr, "Got entry %d\n", entry - &Data[0]);
#endif
        while (true) {
            // entry can not be in COPYING if table is not full, 
            // but there can be small possible overhead (see 4)
            if (entry->m_state == COPYING)
                Copy(entry);
            if (entry->m_state == COPIED)
                return FAILED;

            if (entry->m_value == EntryT::NO_VALUE) {
                if (cond.m_when == PutCondition::IF_EXISTS)
                    return FAILED;
                if (AtomicCas(&entry->m_value, value, EntryT::NO_VALUE) && entry->m_state == NORMAL) { // check that entry wasn't copied during assignment
                    ++m_parent->m_guard->m_putWrites;
                    return SUCCEEDED; 
                }
            }
            if (cond.m_when == PutCondition::IF_ABSENT)
                return FAILED;
            
            // we can set this value no matter what state is entry in
            Atomic oldValue = entry->m_value;
            if (cond.m_when == PutCondition::IF_MATCHES && oldValue != cond.m_value)
                return FAILED;
            if (AtomicCas(&entry->m_value, value, oldValue) && entry->m_state == NORMAL) {
                return SUCCEEDED;
            }
        }
    }

    template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
    typename Table<H, E, RK1, RK2, RV>::EResult Table<H, E, RK1, RK2, RV>::Delete(Atomic key) {
#ifdef TRACE
        Trace(std::cerr, "Delete \"%s\"\n", key);
#endif

        size_t hashValue = m_parent->m_hash(key);
        AtomicBase foundKey;
        EntryT* entry = LookUp(key, hashValue, foundKey);

        //  if table is full we can't say anything
        if (!entry)
            return FULL_TABLE;

        while (true) {
            if (entry->m_state == COPYING)
                Copy(entry);
            if (entry->m_state == COPIED)
                return FULL_TABLE;

            if (foundKey == EntryT::NO_KEY) {
                // if table is full, entry can be in the next table
                if (IsFull())
                    return FULL_TABLE;
                // otherwise we can suppose there is no such key in table
                // but nothing was really deleted
                return FAILED;
            }            

            entry->m_key = EntryT::TOMBSTONE;
            if (entry->m_state == NORMAL)
                return SUCCEEDED;
        }
    }

    template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
    void Table<H, E, RK1, RK2, RV>::DoCopyTask() {
        if ((size_t)m_copiedCnt < m_size) {
            size_t finish = AtomicAdd(m_copiedCnt, m_copyTaskSize);
            size_t start = finish - m_copyTaskSize;
            if (start < m_size) {
                finish = Min(m_size, finish);
#ifdef TRACE
                Trace(std::cerr, "Copy from %d to %d\n", start, finish);
#endif        
                for (size_t i = start; i < finish; ++i)
                    Copy(&m_data[i]);
            }
        }

        if ((size_t)m_copiedCnt >= m_size)
            PrepareToDelete();
    }

    template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
    void Table<H, E, RK1, RK2, RV>::PrepareToDelete() {
#ifdef TRACE
        Trace(std::cerr, "PrepareToDelete\n");
#endif
        AtomicBase currentTableNumber = m_parent->m_tableNumber;
        if (m_parent->m_head == this && AtomicCas(&m_parent->m_head, m_next, this)) {
            // deleted table from main list
            // now it's only thread that has pointer to it
            AtomicIncrement(m_parent->m_tableNumber);
            m_parent->m_tableToDeleteNumber = currentTableNumber;
            while (true) {
                Table* toDelete = m_parent->m_toDelete;
                m_nextToDelete = toDelete;
                if (AtomicCas(&m_parent->m_toDelete, this, toDelete)) {
#ifdef TRACE_MEM
                    Trace(std::cerr, "Scheduled to delete table %zd\n", (size_t)this);
#endif
                    break;
                }
            }
        }
    }

    template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
    void Table<H, E, RK1, RK2, RV>::Print(std::ostream& ostr) {
        assert(false);
        /*
        std::stringstream buf;

        buf << "Table at " << (size_t)this << ":\n";

        buf << "Size: " << m_size << '\n'
            << "CopiedCnt: " << m_copiedCnt << '\n'
            << "CopyTaskSize: " << m_copyTaskSize << '\n';

        for (size_t i = 0; i < m_size; i++)
            buf << "Entry " << i << ": "
                << "(" << EntryT::KeyToString(m_data[i].m_key)
                << "; " << EntryT::ValueToString(m_data[i].m_value)
                << "; " << EntryT::StateToString(m_data[i].m_state)
                << ")\n";

        ostr << buf.Str();
        */
    }
   
    template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
    void Table<H, E, RK1, RK2, RV>::Trace(std::ostream& ostr, const char* format, ...) {
        assert(false);
        /*
        std::string buf1;
        sprintf(buf1, "Thread %zd in table %zd: ", (size_t)&errno, (size_t)this);

        std::string buf2;
        va_list args;
        va_start(args, format);
        vsprintf(buf2, format, args);
        va_end(args);

        ostr << buf1 + buf2;
        */
    }

    template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
    void ConstIterator<H, E, RK1, RK2, RV>::NextEntry() {
        ++m_index;

        while (m_table) {
            for (; m_index < m_table->m_size; ++m_index) {
                // it's rather fast to copy small entry
                const EntryT& e = m_table->m_data[m_index];
                if (e.m_key != EntryT::NO_KEY && e.m_key != EntryT::TOMBSTONE && e.m_value != EntryT::NO_VALUE && e.m_state == NORMAL)
                    break;
            }
            if (m_index < m_table->m_size)
                break;
            m_index = 0;
            m_table = m_table->m_next;
        }
    }
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
THREAD_LOCAL NLFHT::Guard* LFHashTable<H, E, RK1, RK2, RV>::m_guard;

static inline size_t NextTwoPower(size_t v)
{
    ++v;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    ++v;
    return v;
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
LFHashTable<H, E, RK1, RK2, RV>::LFHashTable(double density, size_t initialSize, const Hash& hash, const Equal& equal)
    : m_hash(hash)
    , m_equal(equal)
    , m_tableNumber(0)
    , m_tableToDeleteNumber((AtomicBase)(-1))
    , m_toDelete(0)
    , m_density(density)
    , m_size(0)
{
    initialSize = NextTwoPower(initialSize);
    m_head = new Table(this, initialSize); // size must be power of 2
#ifdef TRACE
    Trace(std::cerr, "TLFHashTable created\n");
#endif
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
LFHashTable<H, E, RK1, RK2, RV>::LFHashTable(size_t initialSize)
{
    LFHashTable<H, E, RK1, RK2, RV>(0.3, initialSize, H(), E());
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
LFHashTable<H, E, RK1, RK2, RV>::LFHashTable()
{
    LFHashTable(1);
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
AtomicBase LFHashTable<H, E, RK1, RK2, RV>::Get(Atomic key)
{
    assert(key != Entry::NO_KEY);

    StartGuarding();

#ifdef TRACE
    Trace(std::cerr, "Get \"%s\"\n", key);
#endif

    size_t hashValue = Hash(key);

    AtomicBase returnValue = Entry::NO_VALUE;
    for (Table* cur = m_head; cur; cur = cur->GetNext()) {
        AtomicBase foundKey;
        Entry* entry = cur->LookUp(key, hashValue, foundKey);

        if (foundKey == Entry::NO_KEY) {
            // we can't insert in full table, if table is full we should continue search
            // but if it's not full, NO_KEY means we found an empty entry
            if (!cur->IsFull()) 
                break;
        }
        else {
            if (entry->m_state == NLFHT::COPYING)
                cur->Copy(entry); 
            if (entry->m_state != NLFHT::COPIED) {
                returnValue = entry->m_value;
                break;
            }
        }

        // if state is SHADOW, we should either find 
    }

    StopGuarding();
    return returnValue; 
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
bool LFHashTable<H, E, RK1, RK2, RV>::Find(Atomic key, Atomic& value)
{
    assert(key != Entry::NO_KEY);

    StartGuarding();

#ifdef TRACE
    Trace(std::cerr, "Get \"%s\"\n", key);
#endif

    size_t hashValue = m_hash(key);

    bool result = false;
    for (Table* cur = m_head; cur; cur = cur->GetNext())
    {
        AtomicBase foundKey;
        Entry* entry = cur->LookUp(key, hashValue, foundKey);

        if (foundKey == Entry::NO_KEY)
        {
            // we can't insert in full table, if table is full we should continue search
            // but if it's not full, NO_KEY means we found an empty entry
            if (!cur->IsFull())
                break;
        }
        else
        {
            if (entry->m_state == NLFHT::COPYING)
                cur->Copy(entry);
            if (entry->m_state != NLFHT::COPIED) {
                value = entry->m_value;
                result = true;
            }
        }
    }

    StopGuarding();
    return result;
}


// returns true if new key appeared in a table
template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
bool LFHashTable<H, E, RK1, RK2, RV>::Put(Atomic key, Atomic value, PutCondition cond) {
    assert(key != Entry::NO_KEY);
    assert(value != Entry::NO_VALUE);

    if (cond.m_when == PutCondition::IF_MATCHES && cond.m_value == Entry::NO_VALUE)
        cond.m_when = PutCondition::IF_ABSENT;

    StartGuarding();

#ifdef TRACE
    Trace(std::cerr, "Put key \"%s\" and value \"%s\" under condition %s..\n",
            TEntry::KeyToString(key).c_str(), TEntry::ValueToString(value).c_str(),
            ~cond.ToString());
#endif
    Table* cur = m_head;
    typename Table::EResult result;
    while (true)
    {
        if ((result = cur->Put(key, value, cond)) != Table::FULL_TABLE) {
            AtomicIncrement(m_size);
            break;
        }
        if (!cur->GetNext())
        {
#ifdef TRACE
            Trace(std::cerr, "Create next table to put new key\n");
#endif            
            cur->CreateNext();
        }
        cur->DoCopyTask();
        cur = cur->GetNext();
    }
    
    StopGuarding();
    TryToDelete();

    return result == Table::SUCCEEDED;
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
bool LFHashTable<H, E, RK1, RK2, RV>::PutIfMatch(AtomicBase key, AtomicBase oldValue, AtomicBase newValue)
{
    return Put(key, newValue, PutCondition(PutCondition::IF_MATCHES, oldValue));
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
bool LFHashTable<H, E, RK1, RK2, RV>::PutIfAbsent(AtomicBase key, AtomicBase newValue)
{
    return Put(key, newValue, PutCondition(PutCondition::IF_ABSENT));
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
bool LFHashTable<H, E, RK1, RK2, RV>::PutIfExists(AtomicBase key, AtomicBase newValue)
{
    return Put(key, newValue, PutCondition(PutCondition::IF_EXISTS));
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
bool LFHashTable<H, E, RK1, RK2, RV>::Delete(Atomic key)
{
    assert(key != Entry::NO_KEY);

    StartGuarding();

#ifdef TRACE
    Trace(std::cerr, "Delete \"%s\"\n", key);
#endif
    Table* cur = m_head;
    typename Table::EResult result;
    while (true)
    {
        if ((result = cur->Delete(key)) != Table::FULL_TABLE)
            break;
        if (!cur->GetNext())
            break;
        cur = cur->GetNext();
    }

    StopGuarding();
    TryToDelete();

    if (result == Table::SUCCEEDED)
    {
        AtomicDecrement(m_size);
        return true;
    }
    else
    {
        return false;
    }
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
void LFHashTable<H, E, RK1, RK2, RV>::Clear()
{
}


template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
void LFHashTable<H, E, RK1, RK2, RV>::StartGuarding() {
    // Guard is thread local storage. &Guard is unique for each thread.
    // Thus, it can be used as identifier.
    if (!m_guard)
        m_guard = m_guardManager.AcquireGuard((size_t)&m_guard);

    while (true) {
        AtomicBase currentTableNumber = m_tableNumber;
#ifdef TRACE
        Trace(std::cerr, "Try to guard table %lld\n", currentTableNumber);
#endif
        m_guard->GuardTable(currentTableNumber);
        AtomicBarrier();
        if (m_tableNumber == currentTableNumber) {
#ifdef TRACE
            Trace(std::cerr, "Started guarding\n");
#endif
            // Atomic operation means memory barrier.
            // Now we are sure, that no thread can delete current Head.
            return;
        }
    }
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
void LFHashTable<H, E, RK1, RK2, RV>::StopGuarding() {
    // See comments in StartGuarding.
    if (!m_guard)
        m_guard = m_guardManager.AcquireGuard((size_t)&m_guard);

    m_guard->StopGuarding();
#ifdef TRACE
    Trace(srd::cerr, "Stopped guarding\n");
#endif
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
void LFHashTable<H, E, RK1, RK2, RV>::TryToDelete() {
#ifdef TRACE
    Trace(std::cerr, "TryToDelete\n");
#endif
    Table* toDel = m_toDelete;
    if (!toDel) 
        return;
    Table* oldHead = m_head;
    AtomicBase firstGuardedTable = m_guardManager.GetFirstGuardedTable();

    // if the following is true, it means that no thread works
    // with the tables to ToDelete list
#ifdef TRACE
    Trace(std::cerr, "TableToDeleteNumber %lld, firstGuardedTable %lld\n", m_tableToDeleteNumber, firstGuardedTable);
#endif
    if (m_tableToDeleteNumber < firstGuardedTable)
        if (AtomicCas(&m_toDelete, (Table*)0, toDel)) {
            if (m_head == oldHead) {
                while (toDel) {
                    Table* nextToDel = toDel->m_nextToDelete;
#ifdef TRACE_MEM
                    Trace(std::cerr, "Deleted table %zd of size %zd\n", (size_t)toDel, toDel->Size);
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
                Table* head = toDel;
                Table* tail;
                for (tail = head; tail->m_next; tail = tail->m_next);

                while (true) {
                    Table* oldToDelete = m_toDelete;
                    tail->m_nextToDelete = oldToDelete;
                    if (AtomicCas(&m_toDelete, head, oldToDelete))
                        break;
                }
#ifdef TRACE_MEM
                Trace(std::cerr, "In fear of ABA problem put tables back to list\n");
#endif
            }
        }
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
void LFHashTable<H, E, RK1, RK2, RV>::Print(std::ostream& ostr) {
    ostr << "TLFHashTable printout\n";
 
    ostr << '\n';

    Table* cur = m_head;
    while (cur) {
        cur->Print(ostr);
        Table* next = cur->GetNext();
        if (next)
            ostr << "---------------\n";
        cur = next;
    }

    ostr << "ToDelete: " << (size_t)m_toDelete << '\n';

    ostr << '\n';
}


template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
void LFHashTable<H, E, RK1, RK2, RV>::Trace(std::ostream& ostr, const char* format, ...) {
    /*
    std::string buf1;
    sprintf(buf1, "Thread %zd: ", (size_t)&errno);

    std::string buf2;
    va_list args;
    va_start(args, format);
    vsprintf(buf2, format, args);
    va_end(args);

    ostr << buf1 + buf2;
    */
}

template <class H, class E, Atomic RK1, Atomic RK2, Atomic RV>
NLFHT::ConstIterator<H, E, RK1, RK2, RV> LFHashTable<H, E, RK1, RK2, RV>::Begin() const {
    ConstIterator begin(m_head, -1);
    ++begin;
    return begin;
}
