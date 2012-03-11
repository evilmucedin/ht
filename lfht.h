#pragma once

#include "table.h"
#include "guards.h"
#include "managers.h"

#include <cstdlib>
#include <cmath>
#include <limits>
#include <memory>
#include <iostream>

namespace NLFHT
{
    class Registrable
    {
    public:
        virtual void RegisterThread() = 0;
        virtual void ForgetThread() = 0;
    };

    class LFHashTableBase : public Registrable, public Guardable
    {
    };

    template <template <class> class T>
    class Proxy
    {
    public:
        template <class U>
        class TRedirected : public T<U>
        {
        public:
            template <class P>
            TRedirected(P param)
                : T<U>(param)
            {
            }
        };
    };

    template <class T>
    class Guarding
    {
    public:
        typedef typename T::TSearchHint SearchHint;

        Guarding(T& table, SearchHint* hint)
            : m_Table(table)
        {
            m_Table.StartGuarding(hint);
        }

        ~Guarding()
        {
            m_Table.StopGuarding();
        }

    private:
        T& m_Table;
    };

    template <class Prt>
    class ConstIterator
    {
    public:
        friend class Prt::Self;

        typedef typename Prt::Self Parent;
        typedef typename Parent::Table Table;
        typedef typename Table::ConstIteratorT TableConstIterator;

        typedef typename Parent::Key TKey;
        typedef typename Parent::Value TValue;

        ConstIterator(const Parent* parent)
            : Impl(parent->m_Head->Begin())
        {
        }

        inline TKey Key() const
        {
            return Impl.Key();
        }

        inline TValue Value() const
        {
            return Impl.Value();
        }

        ConstIterator& operator ++ (int)
        {
            NextEntry();
            return *this;
        }
        ConstIterator& operator ++ ()
        {
            NextEntry();
            return *this;
        }

        bool IsValid() const
        {
            return Impl.IsValid();
        }

    private:
        void NextEntry()
        {
            ++Impl;
            if (!Impl.IsValid())
            {
                Table* NextTable = Impl.GetParent()->GetNext();
                if (NextTable)
                    Impl = NextTable->Begin();
            }
        }

    private:
        TableConstIterator Impl;
    };
}

class TLFHTRegistration : NonCopyable
{
private:
    NLFHT::Registrable& m_Table;

public:
    TLFHTRegistration(NLFHT::Registrable& table)
       : m_Table(table)
    {
        m_Table.RegisterThread();
    }

    ~TLFHTRegistration()
    {
        m_Table.ForgetThread();
    }
};

template <
    typename K,
    typename Val,
    class KeyCmp = EqualToF<K>,
    class HashFn = HashF<K>,
    class ValCmp = EqualToF<Val>,
    class Alloc = DEFAULT_ALLOCATOR(Val),
    class KeyMgr = NLFHT::Proxy<NLFHT::DefaultKeyManager>,
    class ValMgr = NLFHT::Proxy<NLFHT::DefaultValueManager>
>
class LFHashTable : public NLFHT::LFHashTableBase
{
public:
    typedef LFHashTable<K, Val, KeyCmp, HashFn, ValCmp, Alloc, KeyMgr, ValMgr> Self;

    friend class NLFHT::Guarding<Self>;
    friend class NLFHT::Table<Self>;
    friend class NLFHT::ConstIterator<Self>;

    typedef K Key;
    typedef Val Value;
    typedef KeyCmp KeyComparator;
    typedef ValCmp ValueComparator;
    typedef Alloc Allocator;
    typedef typename KeyMgr::template TRedirected<Self> KeyManager;
    typedef typename ValMgr::template TRedirected<Self> ValueManager;

    typedef NLFHT::KeyTraits<Key> HTKeyTraits;
    typedef NLFHT::ValueTraits<Value> THTValueTraits;

    typedef typename NLFHT::HashFunc<Key, HashFn> HashFunc;
    typedef typename NLFHT::KeysAreEqual<Key, KeyComparator> KeysAreEqual;
    typedef typename NLFHT::ValuesAreEqual<Value, ValueComparator> ValuesAreEqual;

    typedef NLFHT::Table<Self> Table;

    typedef NLFHT::Guard<Self> Guard;
    typedef NLFHT::GuardManager<Self> GuardManager;

    typedef typename NLFHT::Entry<Key, Value> Entry;
    typedef typename NLFHT::ConstIterator<Self> ConstIterator;

    typedef typename Allocator::template rebind<Table>::other TableAllocator;

    // class incapsulates CAS possibility
    struct PutCondition
    {
        enum EWhenToPut
        {
            ALWAYS,
            IF_ABSENT, // put if THERE IS NO KEY in table. Can put only NONE in this way.
            IF_EXISTS, // put if THERE IS KEY
            IF_MATCHES, // put if THERE IS KEY and VALUE MATCHES GIVEN ONE

            COPYING // reserved for TTable internal use
        };

        EWhenToPut m_When;
        Value m_Value;

        PutCondition(EWhenToPut when = ALWAYS, Value value = ValueNone())
            : m_When(when)
            , m_Value(value)
        {
        }

        // TO DEBUG ONLY
        std::string ToString() const
        {
            std::stringstream tmp;
            if (m_When == ALWAYS)
                tmp << "ALWAYS";
            else if (m_When == IF_EXISTS)
                tmp << "IF_EXISTS";
            else if (m_When == IF_ABSENT)
                tmp << "IF_ABSENT";
            else
                tmp << "IF_MATCHES";
            tmp << " with " << ValueToString(m_Value);
            return tmp.str();
        }
    };

    class SearchHint
    {
        public:
            friend class LFHashTable<Key, Val, KeyCmp, HashFn, ValCmp, Alloc, KeyMgr, ValMgr>;
            friend class NLFHT::Table< LFHashTable<Key, Val, KeyCmp, HashFn, ValCmp, Alloc, KeyMgr, ValMgr> >;

        public:
            SearchHint()
                : m_Guard(0)
                , m_Table(0)
            {
            }

        private:
            Guard* m_Guard;

            AtomicBase m_TableNumber;
            Table* m_Table;
            Entry* m_Entry;
            bool m_KeySet;

        private:
            SearchHint(AtomicBase tableNumber, Table* table, Entry* entry, bool keySet)
                : m_Guard(0)
                , m_TableNumber(tableNumber)
                , m_Table(table)
                , m_Entry(entry)
                , m_KeySet(keySet)
            {
            }
    };

public:
    LFHashTable(size_t initialSize = 1, double density = 0.5,
                 const KeyComparator& keysAreEqual = KeyCmp(),
                 const HashFn& hash = HashFn(),
                 const ValueComparator& valuesAreEqual = ValCmp());
    LFHashTable(const LFHashTable& other);

    // NotFound value getter to compare with
    inline static Value NotFound()
    {
        return ValueNone();
    }

    // return NotFound value if there is no such key
    Value Get(Key key, SearchHint* hint = 0);
    // returns true if condition was matched
    void Put(Key key, Value value, SearchHint* hint = 0);
    bool PutIfMatch(Key key, Value newValue, Value oldValue, SearchHint *hint = 0);
    bool PutIfAbsent(Key key, Value value, SearchHint* hint = 0);
    bool PutIfExists(Key key, Value value, SearchHint* hint = 0);

    template <class OtherTable>
    void PutAllFrom(const OtherTable& other);

    // returns true if key was really deleted
    bool Delete(Key key, SearchHint* hint = 0);
    bool DeleteIfMatch(Key key, Value oldValue, SearchHint* hint = 0);

    // assume, that StartGuarding and StopGuarding are called by client (by creating TGuarding on stack)
    Value GetNoGuarding(Key key, SearchHint* hint = 0);

    void PutNoGuarding(Key key, Value value, SearchHint* hint = 0);
    bool PutIfMatchNoGuarding(Key key, Value newValue, Value oldValue, SearchHint *hint = 0);
    bool PutIfAbsentNoGuarding(Key key, Value value, SearchHint* hint = 0);
    bool PutIfExistsNoGuarding(Key key, Value value, SearchHint* hint = 0);

    bool DeleteNoGuarding(Key key, SearchHint* hint = 0);
    bool DeleteIfMatchNoGuarding(Key key, Value oldValue, SearchHint* hint = 0);

    // massive operations
    template <class OtherTable>
    void PutAllFromNoGuarding(const OtherTable& other)
    {
        return m_ValuesAreEqual.GetImpl();
    }

    size_t Size() const;
    bool Empty() const
    {
        return Size() == 0;
    }
    ConstIterator Begin() const;

    KeyComparator GetKeyComparator() const
    {
        return m_KeysAreEqual.GetImpl();
    }
    ValueComparator GetValueComparator() const
    {
        return m_ValuesAreEqual.GetImpl();
    }
    HashFn GetHashFunction() const
    {
        return m_Hash.GetImpl();
    }

    Guard& GuardRef() {
        return *m_Guard;
    }
    GuardManager& GuardManagerRef()
    {
        return m_GuardManager;
    }
    KeyManager& KeyManagerRef()
    {
        return m_KeyManager;
    }
    ValueManager& ValueManagerRef()
    {
        return m_ValueManager;
    }

    virtual void RegisterThread()
    {
        NLFHT::ThreadGuardTable::RegisterTable(this);
        m_KeyManager.RegisterThread();
        m_ValueManager.RegisterThread();
    }
    virtual void ForgetThread()
    {
        m_ValueManager.ForgetThread();
        m_KeyManager.ForgetThread();
        NLFHT::ThreadGuardTable::ForgetTable(this);
    }
    virtual NLFHT::BaseGuard* AcquireGuard()
    {
        return m_GuardManager.AcquireGuard();
    }

    Table* GetHead()
    {
        return m_Head;
    }
    Table* GetHeadToDelete()
    {
        return m_HeadToDelete;
    }

    // JUST TO DEBUG
    void Print(std::ostream& ostr);
    void PrintStatistics(std::ostream& str)
    {
        m_GuardManager.PrintStatistics(str);
    }

private:
    class THeadWrapper : public NLFHT::VolatilePointerWrapper<Table>
    {
    public:
        THeadWrapper(LFHashTable* parent)
            : m_Parent(parent)
        {
        }

        inline THeadWrapper& operator= (Table* table)
        {
            Set(table);
            return *this;
        }

        ~THeadWrapper()
        {
            Table* current = Get();
            while (current) {
                Table* tmp = current;
                current = current->GetNext();
                m_Parent->DeleteTable(tmp);
            }
#ifndef NDEBUG
            if (m_Parent->m_TablesCreated != m_Parent->m_TablesDeleted)
            {
                std::cerr << "TablesCreated " << m_Parent->m_TablesCreated << '\n'
                     << "TablesDeleted " << m_Parent->m_TablesDeleted << '\n';
                VERIFY(false, "Some table lost\n");
            }
#endif
        }
    private:
        LFHashTable* m_Parent;
    };

    class THeadToDeleteWrapper : public NLFHT::VolatilePointerWrapper<Table>
    {
    public:
        THeadToDeleteWrapper(LFHashTable* parent)
            : m_Parent(parent)
        {
        }

        inline THeadToDeleteWrapper& operator=(Table* table)
        {
            Set(table);
            return *this;
        }

        ~THeadToDeleteWrapper()
        {
            Table* current = Get();
            while (current)
            {
                Table* tmp = current;
                current = current->GetNextToDelete();
                m_Parent->DeleteTable(tmp);
            }
        }
    private:
        LFHashTable* m_Parent;
    };

    // is used by TTable
    double m_Density;

    // functors
    HashFunc m_Hash;
    KeysAreEqual m_KeysAreEqual;
    ValuesAreEqual m_ValuesAreEqual;

    // allocators
    TableAllocator m_TableAllocator;

    // whole table structure
    THeadWrapper m_Head;
    THeadToDeleteWrapper m_HeadToDelete;

    // guarding
    static NLFHT_THREAD_LOCAL Guard* m_Guard;
    GuardManager m_GuardManager;

    // managers
    KeyManager m_KeyManager;
    ValueManager m_ValueManager;

    // number of head table (in fact it can be less or equal)
    Atomic m_TableNumber;
    // number of head table of HeadToDelete list (in fact it can be greater or equal)
    Atomic m_TableToDeleteNumber;

#ifndef NDEBUG
    // TO DEBUG LEAKS
    Atomic m_TablesCreated;
    Atomic m_TablesDeleted;
#endif

private:
    template <bool ShouldSetGuard>
    Value GetImpl(const Key& key, SearchHint* hint = 0);

    template <bool ShouldSetGuard, bool ShouldDeleteKey>
    bool PutImpl(const Key& key, const Value& value, const PutCondition& condition, SearchHint* hint = 0);

    // thread-safefy and lock-free memory reclamation is done here
    inline void StopGuarding();
    inline void StartGuarding(SearchHint* hint);

    void TryToDelete();

    // allocators usage wrappers
    Table* CreateTable(LFHashTable* parent, size_t size) {
        Table* newTable = m_TableAllocator.allocate(1);
        try
        {
            new (newTable) Table(parent, size);
            newTable->m_AllocSize = size;
            return newTable;
        }
        catch (...)
        {
            m_TableAllocator.deallocate(newTable, size);
            throw;
        }
    }
    void DeleteTable(Table* table, bool shouldDeleteKeys = false) {
#ifdef TRACE
        Trace(Cerr, "DeleteTable %zd\n", (size_t)table);
#endif
        if (shouldDeleteKeys)
        {
            for (typename Table::AllKeysConstIterator it = table->BeginAllKeys(); it.IsValid(); ++it)
            {
                UnRefKey(it.Key());
            }
        }
        m_TableAllocator.destroy(table);
        m_TableAllocator.deallocate(table, table->m_AllocSize);
    }

    // destructing
    void Destroy();

    // traits wrappers
    static Key KeyNone()
    {
        return HTKeyTraits::None();
    }
    void UnRefKey(Key key, size_t cnt = 1)
    {
        m_KeyManager.UnRef(key, cnt);
    }

    static Value ValueNone() {
        return THTValueTraits::None();
    }
    static Value ValueBaby() {
        return THTValueTraits::Baby();
    }
    static Value ValueDeleted() {
        return THTValueTraits::Deleted();
    }
    void UnRefValue(Value value, size_t cnt = 1)
    {
        m_ValueManager.UnRef(value, cnt);
    }

    // guard getting wrapper
    Guard* GuardForTable()
    {
        return dynamic_cast<Guard*>(NLFHT::ThreadGuardTable::ForTable(this));
    }

    // JUST TO DEBUG
    static std::string KeyToString(const Key& key)
    {
        return NLFHT::KeyToString<Key>(key);
    }
    static std::string ValueToString(const Value& value)
    {
        return NLFHT::ValueToString<Value>(value);
    }

    void Trace(std::ostream& ostr, const char* format, ...);

    inline void OnPut()
    {
        m_Guard->OnGlobalPut();
    }
    inline void OnGet()
    {
        m_Guard->OnGlobalGet();
    }
};

// need dirty hacks to avoid problems with macros that accept template as a parameter

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
NLFHT_THREAD_LOCAL NLFHT::Guard< LFHashTable<K, V, KC, HF, VC, A, KM, VM> >* LFHashTable<K, V, KC, HF, VC, A, KM, VM>::m_Guard((Guard*)0);

template <typename K, typename V, class KC, class HashFn, class VC, class A, class KM, class VM>
LFHashTable<K, V, KC, HashFn, VC, A, KM, VM>::LFHashTable(size_t initialSize, double density,
                                 const KeyComparator& keysAreEqual,
                                 const HashFn& hash,
                                 const ValueComparator& valuesAreEqual)
    : m_Density(density)
    , m_Hash(hash)
    , m_KeysAreEqual(keysAreEqual)
    , m_ValuesAreEqual(valuesAreEqual)
    , m_Head(this)
    , m_HeadToDelete(this)
    , m_GuardManager(this)
    , m_KeyManager(this)
    , m_ValueManager(this)
    , m_TableNumber(0)
    , m_TableToDeleteNumber(std::numeric_limits<AtomicBase>::max())
#ifndef NDEBUG
    , m_TablesCreated(0)
    , m_TablesDeleted(0)
#endif
{
    assert(m_Density > 1e-9);
    assert(m_Density < 1.);
    assert(initialSize);
    m_Head = CreateTable(this, initialSize/m_Density);
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
LFHashTable<K, V, KC, HF, VC, A, KM, VM>::LFHashTable(const LFHashTable& other)
    : m_Density(other.m_Density)
    , m_Hash(other.m_Hash)
    , m_KeysAreEqual(other.m_KeysAreEqual)
    , m_ValuesAreEqual(other.m_ValuesAreEqual)
    , m_Head(this)
    , m_HeadToDelete(this)
    , GuardManager(this)
    , m_KeyManager(this)
    , m_ValueManager(this)
    , m_TableNumber(0)
    , m_TableToDeleteNumber(std::numeric_limits<AtomicBase>::max())
#ifndef NDEBUG
    , m_TablesCreated(0)
    , m_TablesDeleted(0)
#endif
{
#ifdef TRACE
    Trace(Cerr, "TLFHashTable copy constructor called\n");
#endif
    m_Head = CreateTable(this, Max((size_t)1, other.Size()) / m_Density);
    PutAllFrom(other);
#ifdef TRACE
    Trace(Cerr, "TFLHashTable copy created\n");
#endif
}

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
template <bool ShouldSetGuard>
typename LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::Value
LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::GetImpl(const Key& key, SearchHint* hint) {
    assert(!m_KeysAreEqual(key, KeyNone()));
#ifdef TRACE
    Trace(Cerr, "TLFHashTable.Get(%s)\n", ~KeyToString(key));
#endif

    Guard* lastGuard;
    if (ShouldSetGuard) {
        // Value of Guard should be saved on the stack and then restored.
        // The reason - this method can be called by outer LFH table.
        lastGuard = m_Guard;
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

    if (EXPECT_FALSE(m_Head->GetNext()))
        m_Head->DoCopyTask();

    const size_t hashValue = m_Hash(key);
    Value returnValue;
    Table* cur = m_Head;
    do
    {
        if (cur->Get(key, hashValue, returnValue, hint))
        {
            break;
        }
        cur = cur->GetNext();
    }
    while (cur);

    if (!cur || EXPECT_FALSE(m_ValuesAreEqual(returnValue, ValueBaby())))
    {
        returnValue = NotFound();
    }

    if (ShouldSetGuard)
    {
        StopGuarding();
        m_Guard = lastGuard;
    }

#ifdef TRACE
    Trace(Cerr, "Get returns %s\n", ~ValueToString(returnValue));
#endif
    return returnValue;
}

// returns true if new key appeared in a table
template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
template <bool ShouldSetGuard, bool ShouldDeleteKey>
bool LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::
PutImpl(const Key& key, const Value& value, const PutCondition& cond, SearchHint* hint)
{
    assert(THTValueTraits::IsGood(value));
    assert(!m_KeysAreEqual(key, KeyNone()));
#ifdef TRACE
    Trace(Cerr, "TLFHashTable.Put key \"%s\" and value \"%s\" under condition %s..\n",
            ~KeyToString(key), ~ValueToString(value),
            ~cond.ToString());
#endif

    Guard* lastGuard;
    if (ShouldSetGuard)
    {
        lastGuard = m_Guard;
        StartGuarding(hint);
        OnPut();
    }

    if (EXPECT_FALSE(m_Head->GetNext()))
    {
        m_Head->DoCopyTask();
    }

    typename Table::EResult result;
    bool keyInstalled;

    Table* cur = m_Head;
    size_t cnt = 0;
    while (true)
    {
        if (++cnt >= 100000)
        {
            VERIFY(false, "Too long table list\n");
        }
        if ((result = cur->Put(key, value, cond, keyInstalled)) != Table::FULL_TABLE)
        {
            break;
        }
        if (!cur->GetNext())
        {
            cur->CreateNext();
        }
        cur = cur->GetNext();
    }

    if (ShouldDeleteKey && !keyInstalled)
    {
        UnRefKey(key);
    }
    if (result == Table::FAILED)
    {
        UnRefValue(value);
    }

    if (ShouldSetGuard)
    {
        StopGuarding();
        m_Guard = lastGuard;
    }

    TryToDelete();

    return result == Table::SUCCEEDED;
}

// hash table access methods

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
typename LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::Value
LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::
Get(Key key, SearchHint* hint)
{
    return GetImpl<true>(key, hint);
}

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
void LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::
Put(Key key, Value value, SearchHint* hint)
{
    PutImpl<true, true>(key, value, PutCondition(PutCondition::IF_MATCHES), hint);
}

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::
PutIfMatch(Key key, Value newValue, Value oldValue, SearchHint* hint)
{
    return PutImpl<true, true>(key, newValue, PutCondition(PutCondition::IF_MATCHES, oldValue), hint);
}

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::
PutIfAbsent(Key key, Value value, SearchHint* hint)
{
    return PutImpl<true, true>(key, value, PutCondition(PutCondition::IF_ABSENT, ValueBaby()), hint);
}

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::
PutIfExists(Key key, Value newValue, SearchHint* hint)
{
    return PutImpl<true, true>(key, newValue, PutCondition(PutCondition::IF_EXISTS), hint);
}

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::Delete(Key key, SearchHint* hint) {
    return PutImpl<true, false>(key, ValueNone(), PutCondition(PutCondition::IF_EXISTS), hint);
}

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::DeleteIfMatch(Key key, Value oldValue, SearchHint* hint)
{
    return PutImpl<true, false>(key, ValueNone(), PutCondition(PutCondition::IF_MATCHES, oldValue), hint);
}

// no guarding

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
typename LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::Value
LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::
GetNoGuarding(Key key, SearchHint* hint)
{
    return GetImpl<false>(key, hint);
}

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
void LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::
PutNoGuarding(Key key, Value value, SearchHint* hint)
{
    PutImpl<false, true>(key, value, PutCondition(), hint);
}

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::
PutIfMatchNoGuarding(Key key, Value newValue, Value oldValue, SearchHint* hint)
{
    return PutImpl<false, true>(key, newValue, PutCondition(PutCondition::IF_MATCHES, oldValue), hint);
}

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::
PutIfAbsentNoGuarding(Key key, Value value, SearchHint* hint)
{
    return PutImpl<false, true>(key, value, PutCondition(PutCondition::IF_ABSENT, ValueBaby()), hint);
}

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::
PutIfExistsNoGuarding(Key key, Value newValue, SearchHint* hint)
{
    return PutImpl<false, true>(key, newValue, PutCondition(PutCondition::IF_EXISTS), hint);
}

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::DeleteNoGuarding(Key key, SearchHint* hint)
{
    return PutImpl<false, false>(key, ValueNone(), PutCondition(PutCondition::IF_EXISTS), hint);
}

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
bool LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::DeleteIfMatchNoGuarding(Key key, Value oldValue, SearchHint* hint)
{
    return PutImpl<false, false>(key, ValueNone(), PutCondition(PutCondition::IF_MATCHES, oldValue), hint);
}

// massive put

template <typename Key, typename V, class KC, class HF, class VC, class A, class KM, class VM>
template <class OtherTable>
void LFHashTable<Key, V, KC, HF, VC, A, KM, VM>::PutAllFrom(const OtherTable& other)
{
    TLFHTRegistration registration(*this);
    for (ConstIterator it = other.Begin(); it.IsValid(); ++it)
    {
        Key keyClone = m_KeyManager.CloneAndRef(it.Key());
        Key valueClone = m_ValueManager.CloneAndRef(it.Value());
        Put(keyClone, valueClone);
    }
}

// how to guarp

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
inline void LFHashTable<K, V, KC, HF, VC, A, KM, VM>::StartGuarding(SearchHint* hint)
{
    if (hint) {
        if (EXPECT_FALSE(!hint->m_Guard))
            hint->m_Guard = GuardForTable();
        m_Guard = hint->m_Guard;
    } else {
        m_Guard = GuardForTable();
    }
    VERIFY(m_Guard, "Register in table!\n");
    assert(m_Guard == NLFHT::ThreadGuardTable::ForTable(this));
    assert(m_Guard->GetThreadId() == CurrentThreadId());

    while (true) {
        AtomicBase currentTableNumber = m_TableNumber;
        m_Guard->GuardTable(currentTableNumber);
        AtomicBarrier();
        if (EXPECT_TRUE(m_TableNumber == currentTableNumber)) {
            // Now we are sure, that no thread can delete current Head.
            return;
        }
    }
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
inline void LFHashTable<K, V, KC, HF, VC, A, KM, VM>::StopGuarding()
{
    assert(m_Guard);
    m_Guard->StopGuarding();
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
void LFHashTable<K, V, KC, HF, VC, A, KM, VM>::TryToDelete()
{
    Table* toDel = m_HeadToDelete;
    if (!toDel)
        return;
    Table* oldHead = m_Head;
    AtomicBase firstGuardedTable = m_GuardManager.GetFirstGuardedTable();

    // if the following is true, it means that no thread works
    // with the tables to ToDelete list
    if (m_TableToDeleteNumber < firstGuardedTable)
    {
        if (AtomicCas(&m_HeadToDelete, (Table*)0, toDel))
        {
            if (m_Head == oldHead)
            {
                while (toDel)
                {
                    Table* nextToDel = toDel->m_NextToDelete;
#ifdef TRACE_MEM
                    Trace(Cerr, "Deleted table %zd of size %zd\n", (size_t)toDel, toDel->Size);
#endif
                    DeleteTable(toDel, true);
                    toDel = nextToDel;
                }
            }
            else
            {
                // This is handling of possible ABA problem.
                // If some other table was removed from list,
                // successfull CAS doesn't guarantee that we have
                // the same table as before. We put all the elements back
                // to the ToDelete list.
                Table* head = toDel;
                Table* tail = head;
                while (tail->m_NextToDelete)
                {
                    tail = tail->m_NextToDelete;
                }

                while (true)
                {
                    Table* oldToDelete = m_HeadToDelete;
                    tail->m_NextToDelete = oldToDelete;
                    if (AtomicCas(&m_HeadToDelete, head, oldToDelete))
                    {
                        break;
                    }
                }
            }
        }
    }
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
typename LFHashTable<K, V, KC, HF, VC, A, KM, VM>::ConstIterator
LFHashTable<K, V, KC, HF, VC, A, KM, VM>::Begin() const
{
    return ConstIterator(this);
}

// JUST TO DEBUG

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
void LFHashTable<K, V, KC, HF, VC, A, KM, VM>::Print(std::ostream& ostr)
{
    std::stringstream buf;
    buf << "TLFHashTable printout\n";

    buf << '\n';

    Table* cur = m_Head;
    while (cur)
    {
        cur->Print(buf);
        Table* next = cur->GetNext();
        if (next)
            buf << "---------------\n";
        cur = next;
    }

    buf << "HeadToDelete: " << (size_t)(Table*)m_HeadToDelete << '\n';
    buf << m_KeyManager->ToString() << '\n'
        << m_ValueManager->ToString() << '\n';

    ostr << buf.str() << '\n';
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
size_t LFHashTable<K, V, KC, HF, VC, A, KM, VM>::Size() const
{
    size_t result = 0;
    ConstIterator it = Begin();
    while (it.IsValid())
    {
        ++result;
        ++it;
    }
    return result;
}

template <typename K, typename V, class KC, class HF, class VC, class A, class KM, class VM>
void LFHashTable<K, V, KC, HF, VC, A, KM, VM>::Trace(std::ostream& ostr, const char* format, ...)
{
    char buf1[10000];
    sprintf(buf1, "Thread %zd: ", (size_t)&errno);

    char buf2[10000];
    va_list args;
    va_start(args, format);
    vsprintf(buf2, format, args);
    va_end(args);

    ostr << buf1 << buf2;
}
