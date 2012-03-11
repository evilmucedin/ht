#pragma once

#include "atomic_traits.h"

#include <cerrno>
#include <cmath>
#include <vector>
#include <stdarg.h>

#include "lfht.h"

namespace NLFHT {
    template <class K, class V>
    struct TEntry {
        typedef typename TKeyTraits<K>::TAtomicKey TAtomicKey;
        typedef typename TValueTraits<V>::TAtomicValue TAtomicValue;

        TAtomicKey Key;
        TAtomicValue Value;

        TEntry()
            : Key(TKeyTraits<K>::None())
            , Value(TValueTraits<V>::Baby())
        {
        }
    };

    template <class Prt, bool IterateAllKeys = false>
    class TTableConstIterator;

    template <class Prt>
    class TTable : NonCopyable {
    public:
        friend class Prt::TSelf;
        friend class TTableConstIterator<TTable>;
        friend class TTableConstIterator<TTable, true>;

        typedef typename Prt::TSelf TParent;
        typedef TTable TSelf;

        typedef typename TParent::TKey TKey;
        typedef typename TParent::TValue TValue;

        typedef typename TKeyTraits<TKey>::TAtomicKey TAtomicKey;
        typedef typename TValueTraits<TValue>::TAtomicValue TAtomicValue;

        typedef TEntry<TKey, TValue> TEntryT;
        typedef TTable<TParent> TTableT;
        typedef TTableConstIterator<TSelf> TConstIteratorT;
        typedef TTableConstIterator<TSelf, true> TAllKeysConstIterator;
        typedef typename TParent::TPutCondition TPutCondition;
        typedef typename TParent::TSearchHint TSearchHint;

        enum EResult {
            FULL_TABLE,
            SUCCEEDED,
            FAILED,

            RETRY,
            CONTINUE
        };

    public:
        TTable(TParent* parent, size_t size)
            : Size( FastClp2(size) )
            , SizeMinusOne(Size - 1)
            , MinProbeCnt(Size)
            , IsFullFlag(false)
            , CopiedCnt(0)
            , CopyTaskSize(0)
            , Parent(parent)
            , Next(0)
            , NextToDelete(0)
        {
            VERIFY(Size, "Size must be non-zero\n");
            Data.resize(Size);
            const double tooBigDensity = Min(0.7, 2 * Parent->Density);
            UpperKeyCountBound = Min(Size, (size_t)(ceil(tooBigDensity * Size)));

#ifndef NDEBUG
            AtomicIncrement(Parent->TablesCreated);
#endif
        }

        ~TTable() {
#ifdef TRACE
            Trace(Cerr, "TTable destructor called\n");
#endif
#ifndef NDEBUG
            AtomicIncrement(Parent->TablesDeleted);
#endif
        }

        inline bool IsFull() const {
            return IsFullFlag;
        }

        // getters and setters
        inline TTableT* GetNext() const {
            return Next;
        }
        inline TTableT* GetNextToDelete() const {
            return NextToDelete;
        }

// table access methods
        inline bool GetEntry(TEntryT* entry, TValue& value);
        bool Get(TKey key, size_t hashValue, TValue& value, TSearchHint* hint);

        EResult FetchEntry(TKey key, TEntryT* entry,
                           bool thereWasKey, bool& keyIsInstalled, const TPutCondition& cond);
        EResult PutEntry(TEntryT* entry, TValue value,
                         const TPutCondition& cond, bool updateAliveCnt);
        EResult Put(TKey key, TValue value,
                    const TPutCondition& cond, bool& keyInstalled, bool updateAliveCnt = true);

        TConstIteratorT Begin() const {
            return TConstIteratorT(this);
        }
        TAllKeysConstIterator BeginAllKeys() const {
            return TAllKeysConstIterator(this);
        }

        // JUST TO DEBUG
        void Print(std::ostream& ostr, bool compact = false);

        size_t AllocSize;

    private:
        const size_t Size;
        const size_t SizeMinusOne;
        Atomic MinProbeCnt;
        volatile bool IsFullFlag;
        size_t UpperKeyCountBound;

        Atomic CopiedCnt;
        size_t CopyTaskSize;

        typedef std::vector<TEntryT> TData;
        TData Data;

        TParent* Parent;
        TTableT *volatile Next;
        TTableT *volatile NextToDelete;

        SpinLock Lock;

    private:
        template<bool CheckFull>
        TEntryT* LookUp(TKey key, size_t hash, TKey& foundKey);
        void Copy(TEntryT* entry);

        void CreateNext();
        void PrepareToDelete();
        void DoCopyTask();

        // traits wrappers
        inline static TKey NoneKey() {
            return TKeyTraits<TKey>::None();
        }
        static TValue CopiedValue() {
            return TValueTraits<TValue>::Copied();
        }
        static TValue NoneValue() {
            return TValueTraits<TValue>::None();
        }
        static TValue DeletedValue() {
            return TValueTraits<TValue>::Deleted();
        }
        static TValue BabyValue() {
            return TValueTraits<TValue>::Baby();
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
            return Parent->KeysAreEqual(lft, rgh);
        }
        FORCED_INLINE bool ValuesAreEqual(TValue lft, TValue rgh) const {
            return Parent->ValuesAreEqual(lft, rgh);
        }

        inline static bool IsCopying(TValue value) {
            return TValueTraits<TValue>::IsCopying(value);
        }
        inline static void SetCopying(TAtomicValue& value) {
            TValueTraits<TValue>::SetCopying(value);
        }
        inline static TValue PureValue(TValue value) {
            return TValueTraits<TValue>::PureValue(value);
        }

        inline bool KeysCompareAndSet(TAtomicKey& key, TKey newKey, TKey oldKey) {
            return TKeyTraits<TKey>::CompareAndSet(key, newKey, oldKey);
        }
        inline bool ValuesCompareAndSet(TAtomicValue& value, TValue newValue, TValue oldValue) {
            return TValueTraits<TValue>::CompareAndSet(value, newValue, oldValue);
        }

        inline void UnRefKey(TKey key, size_t cnt = 1) {
            Parent->KeyManager.UnRef(key, cnt);
        }
        inline void ReadValueAndRef(TValue& value, const TAtomicValue& atomicValue) {
            Parent->ValueManager.ReadAndRef(value, atomicValue);
        }
        inline void UnRefValue(TValue value, size_t cnt = 1) {
            Parent->ValueManager.UnRef(value, cnt);
        }

        // guards wrappers
        void ForbidPrepareToDelete() {
            Parent->Guard->ForbidPrepareToDelete();
        }
        void AllowPrepareToDelete() {
            Parent->Guard->AllowPrepareToDelete();
        }
        bool CanPrepareToDelete() {
            return Parent->GuardManager.CanPrepareToDelete();
        }
        void IncreaseAliveCnt() {
            Parent->Guard->IncreaseAliveCnt();
        }
        void DecreaseAliveCnt() {
            Parent->Guard->DecreaseAliveCnt();
        }
        void IncreaseKeyCnt() {
            Parent->Guard->IncreaseKeyCnt();
        }
        void ZeroKeyCnt() {
            Parent->GuardManager.ZeroKeyCnt();
        }

        // JUST TO DEBUG
        void Trace(std::ostream& ostr, const char* format, ...);

        void OnPut() {
            Parent->Guard->OnLocalPut();
        }
        void OnCopy() {
            Parent->Guard->OnLocalCopy();
        }
        void OnLookUp() {
            Parent->Guard->OnLocalLookUp();
        }
        void OnDelete() {
            Parent->Guard->OnLocalDelete();
        }
    };

    // NOT thread-safe iterator, use for moments, when only one thread works with table
    template <class Prt, bool IterateAllKeys>
    class TTableConstIterator {
    public:
        friend class Prt::TSelf;

        typedef typename Prt::TSelf TParent;

        typedef typename TParent::TKey TKey;
        typedef typename TParent::TValue TValue;
        typedef typename TParent::TAtomicKey TAtomicKey;
        typedef typename TParent::TAtomicValue TAtomicValue;

        typedef TEntry<TKey, TValue> TEntryT;

        inline TKey Key() const {
            return Parent->Data[Index].Key;
        }

        inline TValue Value() const {
            return Parent->Data[Index].Value;
        }

        inline const TParent* GetParent() const {
            return Parent;
        }

        TTableConstIterator& operator ++ (int) {
            NextEntry();
            return *this;
        }
        TTableConstIterator& operator ++ () {
            NextEntry();
            return *this;
        }

        bool IsValid() const {
            return Index < Parent->Size;
        }

        TTableConstIterator(const TTableConstIterator& it)
            : Parent(it.Parent)
            , Index(it.Index)
        {
        }

    private:
        const TParent* Parent;
        size_t Index;

    private:
        TTableConstIterator(const TParent* parent)
            : Parent(parent)
            , Index(-1)
        {
            NextEntry();
        }

        // if iterator is not valid, should not change iterator public state
        void NextEntry() {
            ++Index;
            for (; Index < Parent->Size; Index++)
                if (IsValidEntry(Parent->Data[Index]))
                    break;
        }
        bool IsValidEntry(const TEntryT& entry) {
            const TAtomicKey& key = entry.Key;
            TValue value = TValueTraits<TValue>::PureValue(entry.Value);
            if (TKeyTraits<TKey>::IsReserved(key))
                return false;
            if (IterateAllKeys)
                return !ValuesAreEqual(value, TValueTraits<TValue>::Copied());
            else
                return !TValueTraits<TValue>::IsCopying(value) &&
                       !TValueTraits<TValue>::IsReserved(value);
        }

        // traits wrappers
        bool ValuesAreEqual(TValue lft, TValue rgh) {
            return Parent->ValuesAreEqual(lft, rgh);
        }
    };

    template <class Prt>
    template <bool CheckFull>
    typename TTable<Prt>::TEntryT*
    TTable<Prt>::LookUp(TKey key, size_t hash, TKey& foundKey) {
        assert(!KeyIsNone(key));
        OnLookUp();

        typename TData::iterator i = Data.begin() + (hash & SizeMinusOne);
        AtomicBase probeCnt = Size;

        TEntryT* returnEntry;
        do {
            TEntryT& entry = *i;
            const TKey entryKey(entry.Key);

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
            if (EXPECT_FALSE(i == Data.end()))
                i = Data.begin();
            --probeCnt;
        } while (probeCnt);

        if (EXPECT_FALSE(0 == probeCnt)) {
            foundKey = NoneKey();
            returnEntry = 0;
        }

        if (CheckFull) {
            AtomicBase oldCnt;
            while (!IsFullFlag && probeCnt < (oldCnt = MinProbeCnt)) {
                if (AtomicCas(&MinProbeCnt, probeCnt, oldCnt)) {
                    const size_t keysCnt = Parent->GuardManager.TotalKeyCnt();

                    // keysCnt is approximate, that's why we must check that table is absolutely full
                    if (keysCnt >= UpperKeyCountBound) {
                        IsFullFlag = true;
                    }
                }
            }
            // cause TotalKeyCnt is approximate, sometimes table be absotely full, even
            // when previous check fails
            if (!returnEntry && !IsFullFlag)
                IsFullFlag = true;
        }

        return returnEntry;
    }

    // try to take value from entry
    // return false, if entry was copied
    template <class Prt>
    inline bool TTable<Prt>::GetEntry(TEntryT* entry, TValue& value) {
#ifdef TRACE
        Trace(Cerr, "GetEntry in %zd\n", entry - &Data[0]);
#endif
        if (EXPECT_FALSE(IsCopying(TValue(entry->Value))))
            Copy(entry);
        ReadValueAndRef(value, entry->Value);
        const bool canBeInNextTables = ValueIsCopied(value) || ValueIsDeleted(value);
        return !canBeInNextTables;
    }

    // tries to take value corresponding to key from table
    // returns false, if key information was copied
    template <class Prt>
    inline bool TTable<Prt>::Get(TKey key, size_t hashValue, TValue& value, TSearchHint*) {
        TKey foundKey;
        TEntryT* entry = LookUp<false>(key, hashValue, foundKey);

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
    void TTable<Prt>::CreateNext() {
        assert(IsFull());

        Lock.Acquire();
        if (Next) {
            Lock.Release();
            return;
        }

        const size_t aliveCnt = Max((AtomicBase)1, Parent->GuardManager.TotalAliveCnt());
        const size_t nextSize = Max((size_t)1, (size_t)ceil(aliveCnt * (1. / Parent->Density)));
        ZeroKeyCnt();

        Next = Parent->CreateTable(Parent, nextSize);
#ifdef TRACE
        Trace(Cerr, "Table done\n");
#endif
        CopyTaskSize = Max((size_t)logf(Size) + 1, 2 * (Size / (size_t)(Parent->Density * Next->Size + 1)));

        Lock.Release();
    }

    template <class Prt>
    void TTable<Prt>::Copy(TEntry<TKey, TValue>* entry) {
        OnCopy();

        SetCopying(entry->Value);
        // by now entry is locked for modifications (except becoming TOMBSTONE)
        // cause nobody does CAS on copying values

        // remember the value to copy to the next table
        TValue entryValue(PureValue(entry->Value));

        if (ValueIsDeleted(entryValue) || ValueIsCopied(entryValue))
            return;
        if (ValueIsBaby(entryValue)) {
            entry->Value = CopiedValue();
            return;
        }
        if (ValueIsNone(entryValue)) {
            entry->Value = DeletedValue();
            return;
        }

        TTableT* current = this;
        TKey entryKey = entry->Key;
        while (!ValueIsCopied(PureValue(entry->Value))) {
            if (!current->Next)
                current->CreateNext();
            TTableT* target = current->Next;

            bool tmp;
            if (target->Put(entryKey, entryValue, TPutCondition(TPutCondition::COPYING, BabyValue()), tmp, false) != FULL_TABLE)
                entry->Value = CopiedValue();
            else
                current = target;
        }
    }

    template <class Prt>
    typename TTable<Prt>::EResult
    TTable<Prt>::PutEntry(TEntryT* entry, TValue value, const TPutCondition& cond, bool updateCnt) {
#ifdef TRACE
        Trace(Cerr, "PutEntry in entry %zd value %s under condition %s\n", entry - &Data[0],
                     ~ValueToString<TValue>(value), ~cond.ToString());
#endif

        if (EXPECT_FALSE(IsCopying(entry->Value))) {
            Copy(entry);
            return FULL_TABLE;
        }

        const bool shouldRefWhenRead = cond.When == TPutCondition::IF_MATCHES;
        const size_t successRefCnt = shouldRefWhenRead ? 2 : 1;
        const size_t otherRefCnt = successRefCnt - 1;

        TValue oldValue;
        if (shouldRefWhenRead) {
            // we want to compare with oldValue
            // we need guaranty, that it's not deleted
            ReadValueAndRef(oldValue, entry->Value);
        } else {
            oldValue = PureValue(entry->Value);
        }
        if (ValueIsDeleted(oldValue) || ValueIsCopied(oldValue))
        {
            return FULL_TABLE;
        }

        // Good idea to make TPutCondition::When template parameter.
        switch (cond.When) {
            case TPutCondition::COPYING:
                // It's possible to use IF_MATCHES instead, but extra ReadValueAndRef is expensive.
                if (!ValueIsBaby(oldValue))
                    return FAILED;
                break;
            case TPutCondition::IF_ABSENT:
                if (!ValueIsNone(oldValue) && !ValueIsBaby(oldValue))
                    return FAILED;
                break;
            case TPutCondition::IF_MATCHES:
                if (!ValuesAreEqual(oldValue, cond.Value)) {
                    UnRefValue(oldValue, otherRefCnt);
                    return FAILED;
                }
                break;
            case TPutCondition::IF_EXISTS:
                if (ValueIsBaby(oldValue) || ValueIsNone(oldValue))
                    return FAILED;
                break;
            case TPutCondition::ALWAYS:
                break;
            default:
                assert(0);
        }

        if (ValuesCompareAndSet(entry->Value, value, oldValue)) {
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
    typename TTable<Prt>::EResult
    TTable<Prt>::FetchEntry(TKey key, TEntryT* entry, bool thereWasKey, bool& keyInstalled, const TPutCondition& cond) {
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
        TKey entryKey = entry->Key;
        if (KeyIsNone(entryKey)) {
            if (cond.When == TPutCondition::IF_EXISTS ||
                cond.When == TPutCondition::IF_MATCHES)
                return FAILED;
            if (!KeysCompareAndSet(entry->Key, key, NoneKey())) {
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
    typename TTable<Prt>::EResult
    TTable<Prt>::Put(TKey key, TValue value, const TPutCondition& cond, bool& keyInstalled, bool updateAliveCnt) {
        OnPut();

        const size_t hashValue = Parent->Hash(key);
        EResult result = RETRY;

        TEntryT* entry = 0;
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
    void TTable<Prt>::DoCopyTask() {
        if (EXPECT_FALSE(Parent->Head != this))
            return;
        if (EXPECT_FALSE((size_t)CopiedCnt >= Size)) {
            if (CanPrepareToDelete())
                PrepareToDelete();
            return;
        }

        // try to set lock on throwing table away
        ForbidPrepareToDelete();

        // if table is already thrown away your lock is mistake
        if (EXPECT_FALSE(Parent->Head != this)) {
            AllowPrepareToDelete();
            return;
        }

        // help humanity to copy this fucking table
        size_t finish = AtomicAdd(CopiedCnt, CopyTaskSize);
        size_t start = finish - CopyTaskSize;
        if (start < Size) {
            finish = Min(Size, finish);
            for (size_t i = start; i < finish; ++i)
                Copy(&Data[i]);
        }

        // your job is done
        AllowPrepareToDelete();
        // check, maybe it's time to throw away table
        if ((size_t)CopiedCnt >= Size && CanPrepareToDelete())
            PrepareToDelete();
    }

    template <class Prt>
    void TTable<Prt>::PrepareToDelete() {
#ifdef TRACE
        Trace(Cerr, "PrepareToDelete\n");
#endif
        AtomicBase currentTableNumber = Parent->TableNumber;
        if (Parent->Head == this && AtomicCas(&Parent->Head, Next, this)) {
            // deleted table from main list
            // now it's only thread that has pointer to it
            AtomicIncrement(Parent->TableNumber);
            Parent->TableToDeleteNumber = currentTableNumber;
            while (true) {
                TTable* toDelete = Parent->HeadToDelete;
                NextToDelete = toDelete;
                if (AtomicCas(&Parent->HeadToDelete, this, toDelete)) {
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
    void TTable<Owner>::Print(std::ostream& ostr, bool compact) {
        std::stringstream buf;

        buf << "Table at " << (size_t)this << ":\n";

        buf << "Size: " << Size << '\n'
            << "CopiedCnt: " << CopiedCnt << '\n'
            << "CopyTaskSize: " << CopyTaskSize << '\n';

        if (!compact) {
            for (size_t i = 0; i < Size; ++i)
                buf << "Entry " << i << ": "
                    << "(" << KeyToString<TKey>((TKey)Data[i].Key)
                    << "; " << ValueToString<TValue>((TValue)Data[i].Value)
                    << ")\n";
        }

        ostr << buf.str();
    }

    template <class Owner>
    void TTable<Owner>::Trace(std::ostream& ostr, const char* format, ...) {
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
