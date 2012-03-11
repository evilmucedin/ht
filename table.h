#pragma once

#include "atomic_traits.h"

#include <cerrno>
#include <cmath>
#include <vector>
#include <stdarg.h>

#include "lfht.h"

namespace NLFHT {
    template <class K, class V>
    struct Entry
    {
        typedef typename KeyTraits<K>::AtomicKey AtomicKey;
        typedef typename ValueTraits<V>::AtomicValue AtomicValue;

        AtomicKey m_Key;
        AtomicValue m_Value;

        Entry()
            : m_Key(KeyTraits<K>::None())
            , m_Value(ValueTraits<V>::Baby())
        {
        }
    };

    template <class Prt, bool IterateAllKeys = false>
    class TableConstIterator;

    template <class Prt>
    class Table : NonCopyable
    {
    public:
        friend class Prt::Self;
        friend class TableConstIterator<Table>;
        friend class TableConstIterator<Table, true>;

        typedef typename Prt::Self Parent;
        typedef Table Self;

        typedef typename Parent::Key TKey;
        typedef typename Parent::Value TValue;

        typedef typename KeyTraits<TKey>::AtomicKey AtomicKey;
        typedef typename ValueTraits<TValue>::AtomicValue AtomicValue;

        typedef Entry<TKey, TValue> EntryT;
        typedef Table<Parent> TableT;
        typedef TableConstIterator<Self> ConstIteratorT;
        typedef TableConstIterator<Self, true> AllKeysConstIterator;
        typedef typename Parent::TPutCondition PutCondition;
        typedef typename Parent::TSearchHint SearchHint;

        enum EResult {
            FULL_TABLE,
            SUCCEEDED,
            FAILED,

            RETRY,
            CONTINUE
        };

    public:
        Table(Parent* parent, size_t size)
            : m_Size( FastClp2(size) )
            , m_SizeMinusOne(m_Size - 1)
            , m_MinProbeCnt(m_Size)
            , m_IsFullFlag(false)
            , m_CopiedCnt(0)
            , m_CopyTaskSize(0)
            , m_Parent(parent)
            , m_Next(0)
            , m_NextToDelete(0)
        {
            VERIFY(m_Size, "Size must be non-zero\n");
            m_Data.resize(m_Size);
            const double tooBigDensity = Min(0.7, 2 * m_Parent->Density);
            m_UpperKeyCountBound = Min(m_Size, (size_t)(ceil(tooBigDensity * m_Size)));

#ifndef NDEBUG
            AtomicIncrement(m_Parent->TablesCreated);
#endif
        }

        ~Table() {
#ifdef TRACE
            Trace(Cerr, "TTable destructor called\n");
#endif
#ifndef NDEBUG
            AtomicIncrement(m_Parent->TablesDeleted);
#endif
        }

        inline bool IsFull() const
        {
            return m_IsFullFlag;
        }

        // getters and setters
        inline TableT* GetNext() const
        {
            return m_Next;
        }
        inline TableT* GetNextToDelete() const
        {
            return m_NextToDelete;
        }

// table access methods
        inline bool GetEntry(EntryT* entry, TValue& value);
        bool Get(TKey key, size_t hashValue, TValue& value, SearchHint* hint);

        EResult FetchEntry(TKey key, EntryT* entry,
                           bool thereWasKey, bool& keyIsInstalled, const PutCondition& cond);
        EResult PutEntry(EntryT* entry, TValue value,
                         const PutCondition& cond, bool updateAliveCnt);
        EResult Put(TKey key, TValue value,
                    const PutCondition& cond, bool& keyInstalled, bool updateAliveCnt = true);

        ConstIteratorT Begin() const {
            return ConstIteratorT(this);
        }
        AllKeysConstIterator BeginAllKeys() const {
            return AllKeysConstIterator(this);
        }

        // JUST TO DEBUG
        void Print(std::ostream& ostr, bool compact = false);

        size_t m_AllocSize;

    private:
        const size_t m_Size;
        const size_t m_SizeMinusOne;
        Atomic m_MinProbeCnt;
        volatile bool m_IsFullFlag;
        size_t m_UpperKeyCountBound;

        Atomic m_CopiedCnt;
        size_t m_CopyTaskSize;

        typedef std::vector<EntryT> TData;
        TData m_Data;

        Parent* m_Parent;
        TableT *volatile m_Next;
        TableT *volatile m_NextToDelete;

        SpinLock m_Lock;

    private:
        template<bool CheckFull>
        EntryT* LookUp(TKey key, size_t hash, TKey& foundKey);
        void Copy(EntryT* entry);

        void CreateNext();
        void PrepareToDelete();
        void DoCopyTask();

        // traits wrappers
        inline static TKey NoneKey() {
            return KeyTraits<TKey>::None();
        }
        static TValue CopiedValue() {
            return ValueTraits<TValue>::Copied();
        }
        static TValue NoneValue() {
            return ValueTraits<TValue>::None();
        }
        static TValue DeletedValue() {
            return ValueTraits<TValue>::Deleted();
        }
        static TValue BabyValue() {
            return ValueTraits<TValue>::Baby();
        }

        FORCED_INLINE bool ValueIsNone(TValue value) {
            return ValuesAreEqual(value, NoneValue());
        }
        FORCED_INLINE bool ValueIsDeleted(TValue value) {
            return ValuesAreEqual(value, DeletedValue());
        }
        FORCED_INLINE bool ValueIsBaby(TValue value) {
            return ValuesAreEqual(value, BabyValue());
        }
        FORCED_INLINE bool ValueIsCopied(TValue value) {
            return ValuesAreEqual(value, CopiedValue());
        }

        FORCED_INLINE bool KeyIsNone(TKey key) {
            return KeysAreEqual(key, NoneKey());
        }
        FORCED_INLINE bool KeysAreEqual(TKey lft, TKey rgh) const {
            return m_Parent->KeysAreEqual(lft, rgh);
        }
        FORCED_INLINE bool ValuesAreEqual(TValue lft, TValue rgh) const {
            return m_Parent->ValuesAreEqual(lft, rgh);
        }

        inline static bool IsCopying(TValue value) {
            return ValueTraits<TValue>::IsCopying(value);
        }
        inline static void SetCopying(AtomicValue& value) {
            ValueTraits<TValue>::SetCopying(value);
        }
        inline static TValue PureValue(TValue value) {
            return ValueTraits<TValue>::PureValue(value);
        }

        inline bool KeysCompareAndSet(AtomicKey& key, TKey newKey, TKey oldKey) {
            return KeyTraits<TKey>::CompareAndSet(key, newKey, oldKey);
        }
        inline bool ValuesCompareAndSet(AtomicValue& value, TValue newValue, TValue oldValue) {
            return ValueTraits<TValue>::CompareAndSet(value, newValue, oldValue);
        }

        inline void UnRefKey(TKey key, size_t cnt = 1) {
            m_Parent->KeyManager.UnRef(key, cnt);
        }
        inline void ReadValueAndRef(TValue& value, const AtomicValue& atomicValue) {
            m_Parent->ValueManager.ReadAndRef(value, atomicValue);
        }
        inline void UnRefValue(TValue value, size_t cnt = 1) {
            m_Parent->ValueManager.UnRef(value, cnt);
        }

        // guards wrappers
        void ForbidPrepareToDelete() {
            m_Parent->Guard->ForbidPrepareToDelete();
        }
        void AllowPrepareToDelete() {
            m_Parent->Guard->AllowPrepareToDelete();
        }
        bool CanPrepareToDelete() {
            return m_Parent->GuardManager.CanPrepareToDelete();
        }
        void IncreaseAliveCnt() {
            m_Parent->Guard->IncreaseAliveCnt();
        }
        void DecreaseAliveCnt() {
            m_Parent->Guard->DecreaseAliveCnt();
        }
        void IncreaseKeyCnt() {
            m_Parent->Guard->IncreaseKeyCnt();
        }
        void ZeroKeyCnt() {
            m_Parent->GuardManager.ZeroKeyCnt();
        }

        // JUST TO DEBUG
        void Trace(std::ostream& ostr, const char* format, ...);

        void OnPut() {
            m_Parent->Guard->OnLocalPut();
        }
        void OnCopy()
        {
            m_Parent->Guard->OnLocalCopy();
        }
        void OnLookUp()
        {
            m_Parent->Guard->OnLocalLookUp();
        }
        void OnDelete()
        {
            m_Parent->Guard->OnLocalDelete();
        }
    };

    // NOT thread-safe iterator, use for moments, when only one thread works with table
    template <class Prt, bool IterateAllKeys>
    class TableConstIterator
    {
    public:
        friend class Prt::Self;

        typedef typename Prt::Self Parent;

        typedef typename Parent::TKey TKey;
        typedef typename Parent::TValue TValue;
        typedef typename Parent::AtomicKey AtomicKey;
        typedef typename Parent::AtomicValue AtomicValue;

        typedef Entry<TKey, TValue> TEntryT;

        inline TKey Key() const
        {
            return m_Parent->m_Data[m_Index].m_Key;
        }

        inline TValue Value() const
        {
            return m_Parent->m_Data[m_Index].m_Value;
        }

        inline const Parent* GetParent() const
        {
            return m_Parent;
        }

        TableConstIterator& operator++(int)
        {
            NextEntry();
            return *this;
        }
        TableConstIterator& operator++()
        {
            NextEntry();
            return *this;
        }

        bool IsValid() const
        {
            return m_Index < m_Parent->m_Size;
        }

        TableConstIterator(const TableConstIterator& it)
            : m_Parent(it.m_Parent)
            , m_Index(it.m_Index)
        {
        }

    private:
        const Parent* m_Parent;
        size_t m_Index;

    private:
        TableConstIterator(const Parent* parent)
            : m_Parent(parent)
            , m_Index(-1)
        {
            NextEntry();
        }

        // if iterator is not valid, should not change iterator public state
        void NextEntry()
        {
            ++m_Index;
            for (; m_Index < m_Parent->m_Size; ++m_Index)
                if (IsValidEntry(m_Parent->m_Data[m_Index]))
                    break;
        }
        bool IsValidEntry(const TEntryT& entry)
        {
            const AtomicKey& key = entry.m_Key;
            TValue value = ValueTraits<TValue>::PureValue(entry.m_Value);
            if (KeyTraits<TKey>::IsReserved(key))
                return false;
            if (IterateAllKeys)
                return !ValuesAreEqual(value, ValueTraits<TValue>::Copied());
            else
                return !ValueTraits<TValue>::IsCopying(value) &&
                       !ValueTraits<TValue>::IsReserved(value);
        }

        // traits wrappers
        bool ValuesAreEqual(TValue lft, TValue rgh)
        {
            return m_Parent->ValuesAreEqual(lft, rgh);
        }
    };

    template <class Prt>
    template <bool CheckFull>
    typename Table<Prt>::EntryT*
    Table<Prt>::LookUp(TKey key, size_t hash, TKey& foundKey) {
        assert(!KeyIsNone(key));
        OnLookUp();

        typename TData::iterator i = m_Data.begin() + (hash & m_SizeMinusOne);
        AtomicBase probeCnt = m_Size;

        EntryT* returnEntry;
        do {
            EntryT& entry = *i;
            const TKey entryKey(entry.m_Key);

            if (KeysAreEqual(entryKey, key)) {
                foundKey = key;
                returnEntry = &entry;
                break;
            }
            if (KeyIsNone(entryKey)) {
                foundKey = NoneKey();
                returnEntry = &entry;
                break;
            }

            ++i;
            if (EXPECT_FALSE(i == m_Data.end()))
                i = m_Data.begin();
            --probeCnt;
        } while (probeCnt);

        if (EXPECT_FALSE(0 == probeCnt)) {
            foundKey = NoneKey();
            returnEntry = 0;
        }

        if (CheckFull) {
            AtomicBase oldCnt;
            while (!m_IsFullFlag && probeCnt < (oldCnt = m_MinProbeCnt)) {
                if (AtomicCas(&m_MinProbeCnt, probeCnt, oldCnt)) {
                    const size_t keysCnt = m_Parent->GuardManager.TotalKeyCnt();

                    // keysCnt is approximate, that's why we must check that table is absolutely full
                    if (keysCnt >= m_UpperKeyCountBound) {
                        m_IsFullFlag = true;
                    }
                }
            }
            // cause TotalKeyCnt is approximate, sometimes table be absotely full, even
            // when previous check fails
            if (!returnEntry && !m_IsFullFlag)
                m_IsFullFlag = true;
        }

        return returnEntry;
    }

    // try to take value from entry
    // return false, if entry was copied
    template <class Prt>
    inline bool Table<Prt>::GetEntry(EntryT* entry, TValue& value) {
#ifdef TRACE
        Trace(Cerr, "GetEntry in %zd\n", entry - &Data[0]);
#endif
        if (EXPECT_FALSE(IsCopying(TValue(entry->m_Value))))
            Copy(entry);
        ReadValueAndRef(value, entry->m_Value);
        const bool canBeInNextTables = ValueIsCopied(value) || ValueIsDeleted(value);
        return !canBeInNextTables;
    }

    // tries to take value corresponding to key from table
    // returns false, if key information was copied
    template <class Prt>
    inline bool Table<Prt>::Get(TKey key, size_t hashValue, TValue& value, SearchHint*) {
        TKey foundKey;
        EntryT* entry = LookUp<false>(key, hashValue, foundKey);

        // remember current head number before checking copy state
        // AtomicBase tableNumber = Parent->TableNumber;

        bool result;
        const bool keySet = !KeyIsNone(foundKey);
        if (keySet) {
            result = GetEntry(entry, value);
        } else {
            // if table is full we should continue search
            value = NoneValue();
            result = !IsFull();
            // VERIFY(!(!hint && result), "Table %zd failed for key %zd\n", (size_t)this, (size_t)key);
        }

        return result;
    }

    template <class Prt>
    void Table<Prt>::CreateNext() {
        assert(IsFull());

        m_Lock.Acquire();
        if (m_Next) {
            m_Lock.Release();
            return;
        }

        const size_t aliveCnt = Max((AtomicBase)1, m_Parent->GuardManager.TotalAliveCnt());
        const size_t nextSize = Max((size_t)1, (size_t)ceil(aliveCnt * (1. / m_Parent->Density)));
        ZeroKeyCnt();

        m_Next = m_Parent->CreateTable(m_Parent, nextSize);
#ifdef TRACE
        Trace(Cerr, "Table done\n");
#endif
        m_CopyTaskSize = Max((size_t)logf(m_Size) + 1, 2 * (m_Size / (size_t)(m_Parent->Density * m_Next->m_Size + 1)));

        m_Lock.Release();
    }

    template <class Prt>
    void Table<Prt>::Copy(Entry<TKey, TValue>* entry) {
        OnCopy();

        SetCopying(entry->m_Value);
        // by now entry is locked for modifications (except becoming TOMBSTONE)
        // cause nobody does CAS on copying values

        // remember the value to copy to the next table
        TValue entryValue(PureValue(entry->m_Value));

        if (ValueIsDeleted(entryValue) || ValueIsCopied(entryValue))
            return;
        if (ValueIsBaby(entryValue)) {
            entry->m_Value = CopiedValue();
            return;
        }
        if (ValueIsNone(entryValue)) {
            entry->m_Value = DeletedValue();
            return;
        }

        TableT* current = this;
        TKey entryKey = entry->m_Key;
        while (!ValueIsCopied(PureValue(entry->m_Value))) {
            if (!current->m_Next)
                current->CreateNext();
            TableT* target = current->m_Next;

            bool tmp;
            if (target->Put(entryKey, entryValue, PutCondition(PutCondition::COPYING, BabyValue()), tmp, false) != FULL_TABLE)
                entry->m_Value = CopiedValue();
            else
                current = target;
        }
    }

    template <class Prt>
    typename Table<Prt>::EResult
    Table<Prt>::PutEntry(EntryT* entry, TValue value, const PutCondition& cond, bool updateCnt) {
#ifdef TRACE
        Trace(Cerr, "PutEntry in entry %zd value %s under condition %s\n", entry - &Data[0],
                     ~ValueToString<TValue>(value), ~cond.ToString());
#endif

        if (EXPECT_FALSE(IsCopying(entry->m_Value))) {
            Copy(entry);
            return FULL_TABLE;
        }

        const bool shouldRefWhenRead = cond.m_When == PutCondition::IF_MATCHES;
        const size_t successRefCnt = shouldRefWhenRead ? 2 : 1;
        const size_t otherRefCnt = successRefCnt - 1;

        TValue oldValue;
        if (shouldRefWhenRead) {
            // we want to compare with oldValue
            // we need guaranty, that it's not deleted
            ReadValueAndRef(oldValue, entry->m_Value);
        } else {
            oldValue = PureValue(entry->m_Value);
        }
        if (ValueIsDeleted(oldValue) || ValueIsCopied(oldValue))
        {
            return FULL_TABLE;
        }

        // Good idea to make TPutCondition::When template parameter.
        switch (cond.m_When) {
            case PutCondition::COPYING:
                // It's possible to use IF_MATCHES instead, but extra ReadValueAndRef is expensive.
                if (!ValueIsBaby(oldValue))
                    return FAILED;
                break;
            case PutCondition::IF_ABSENT:
                if (!ValueIsNone(oldValue) && !ValueIsBaby(oldValue))
                    return FAILED;
                break;
            case PutCondition::IF_MATCHES:
                if (!ValuesAreEqual(oldValue, cond.m_Value)) {
                    UnRefValue(oldValue, otherRefCnt);
                    return FAILED;
                }
                break;
            case PutCondition::IF_EXISTS:
                if (ValueIsBaby(oldValue) || ValueIsNone(oldValue))
                    return FAILED;
                break;
            case PutCondition::ALWAYS:
                break;
            default:
                assert(0);
        }

        if (ValuesCompareAndSet(entry->m_Value, value, oldValue)) {
            if (updateCnt) {
                bool oldIsAlive = !ValueIsNone(oldValue) && !ValueIsBaby(oldValue);
                bool newIsAlive = !ValueIsNone(value) && !ValueIsBaby(value);
                if (!newIsAlive && oldIsAlive)
                    DecreaseAliveCnt();
                if (newIsAlive && !oldIsAlive)
                    IncreaseAliveCnt();
            }
            UnRefValue(oldValue, successRefCnt);
            // we do not Ref value, so *value can't be used now
            // (it can already be deleted by other thread)
            return SUCCEEDED;
        }

        UnRefValue(oldValue, otherRefCnt);
        return RETRY;
    }

    template <class Prt>
    typename Table<Prt>::EResult
    Table<Prt>::FetchEntry(TKey key, EntryT* entry, bool thereWasKey, bool& keyInstalled, const PutCondition& cond) {
        keyInstalled = false;

        if (!entry)
            return FULL_TABLE;
        if (IsFull()) {
            Copy(entry);
            return FULL_TABLE;
        }

        // if we already know, that key was set
        if (thereWasKey)
            return CONTINUE;

        // if key is NONE, try to get entry
        TKey entryKey = entry->m_Key;
        if (KeyIsNone(entryKey)) {
            if (cond.m_When == PutCondition::IF_EXISTS ||
                cond.m_When == PutCondition::IF_MATCHES)
                return FAILED;
            if (!KeysCompareAndSet(entry->m_Key, key, NoneKey())) {
                return RETRY;
            }

            keyInstalled = true;
            IncreaseKeyCnt();
            return CONTINUE;
        }

        // key was not NONE, if it's not our key, caller should retry fetching
        if (!KeysAreEqual(entryKey, key))
            return RETRY;

        return CONTINUE;
    }

    template <class Prt>
    typename Table<Prt>::EResult
    Table<Prt>::Put(TKey key, TValue value, const PutCondition& cond, bool& keyInstalled, bool updateAliveCnt) {
        OnPut();

        const size_t hashValue = m_Parent->Hash(key);
        EResult result = RETRY;

        EntryT* entry = 0;
#ifndef NDEBUG
        for (size_t cnt = 0; RETRY == result; ++cnt) {
#else
        while (RETRY == result) {
#endif
            TKey foundKey;
            entry = LookUp<true>(key, hashValue, foundKey);
            result = FetchEntry(key, entry, !KeyIsNone(foundKey), keyInstalled, cond);
#ifndef NDEBUG
            if (EXPECT_FALSE(cnt == 10000))
                VERIFY(false, "Fetch hang up\n");
#endif
        }
        if (result != CONTINUE)
            return result;

#ifndef NDEBUG
        for (size_t cnt = 0;
#else
        while (
#endif
        (result = PutEntry(entry, value, cond, updateAliveCnt)) == RETRY
#ifndef NDEBUG
; ++cnt)
#else
)
#endif
        {
#ifndef NDEBUG
            if (EXPECT_FALSE(cnt == 10000))
                VERIFY(false, "Put hang up\n");
#endif
        }
        return result;
    }

    template <class Prt>
    void Table<Prt>::DoCopyTask() {
        if (EXPECT_FALSE(m_Parent->Head != this))
            return;
        if (EXPECT_FALSE((size_t)m_CopiedCnt >= m_Size)) {
            if (CanPrepareToDelete())
                PrepareToDelete();
            return;
        }

        // try to set lock on throwing table away
        ForbidPrepareToDelete();

        // if table is already thrown away your lock is mistake
        if (EXPECT_FALSE(m_Parent->Head != this)) {
            AllowPrepareToDelete();
            return;
        }

        // help humanity to copy this fucking table
        size_t finish = AtomicAdd(m_CopiedCnt, m_CopyTaskSize);
        size_t start = finish - m_CopyTaskSize;
        if (start < m_Size) {
            finish = Min(m_Size, finish);
            for (size_t i = start; i < finish; ++i)
                Copy(&m_Data[i]);
        }

        // your job is done
        AllowPrepareToDelete();
        // check, maybe it's time to throw away table
        if ((size_t)m_CopiedCnt >= m_Size && CanPrepareToDelete())
            PrepareToDelete();
    }

    template <class Prt>
    void Table<Prt>::PrepareToDelete() {
#ifdef TRACE
        Trace(Cerr, "PrepareToDelete\n");
#endif
        AtomicBase currentTableNumber = m_Parent->TableNumber;
        if (m_Parent->Head == this && AtomicCas(&m_Parent->Head, m_Next, this)) {
            // deleted table from main list
            // now it's only thread that has pointer to it
            AtomicIncrement(m_Parent->TableNumber);
            m_Parent->TableToDeleteNumber = currentTableNumber;
            while (true) {
                Table* toDelete = m_Parent->HeadToDelete;
                m_NextToDelete = toDelete;
                if (AtomicCas(&m_Parent->HeadToDelete, this, toDelete)) {
#ifdef TRACE_MEM
                    Trace(Cerr, "Scheduled to delete table %zd\n", (size_t)this);
#endif
                    break;
                }
            }
        }
    }

    // JUST TO DEBUG

    template <class Owner>
    void Table<Owner>::Print(std::ostream& ostr, bool compact) {
        std::stringstream buf;

        buf << "Table at " << (size_t)this << ":\n";

        buf << "Size: " << m_Size << '\n'
            << "CopiedCnt: " << m_CopiedCnt << '\n'
            << "CopyTaskSize: " << m_CopyTaskSize << '\n';

        if (!compact) {
            for (size_t i = 0; i < m_Size; ++i)
                buf << "Entry " << i << ": "
                    << "(" << KeyToString<TKey>((TKey)m_Data[i].Key)
                    << "; " << ValueToString<TValue>((TValue)m_Data[i].Value)
                    << ")\n";
        }

        ostr << buf.str();
    }

    template <class Owner>
    void Table<Owner>::Trace(std::ostream& ostr, const char* format, ...) {
        char buf1[10000];
        sprintf(buf1, "Thread %zd in table %zd: ", (size_t)&errno, (size_t)this);

        char buf2[10000];
        va_list args;
        va_start(args, format);
        vsprintf(buf2, format, args);
        va_end(args);

        ostr << buf1 << buf2;
    }
}
