#pragma once

#include "atomic_traits.h"

#include <cerrno>
#include <cmath>
#include <vector>
#include <stdarg.h>

#include "lfht.h"

namespace NLFHT {
    template<class K, class V>
    struct TEntry {
        typedef typename TKeyTraits<K>::TAtomicKey TAtomicKey;
        typedef typename TValueTraits<V>::TAtomicValue TAtomicValue;

        TAtomicKey Key;
        TAtomicValue Value;

        TEntry() : 
            Key(TKeyTraits<K>::None()),
            Value(TValueTraits<V>::Baby())
        {
        }
    };

    template <class Owner>
    class TConstIterator;

    template <class Owner>
    class TTable {
    public:
        friend class Owner::TSelf;
        friend class TConstIterator<Owner>;

        typedef typename Owner::TSelf TOwner;

        typedef typename TOwner::TKey TKey;
        typedef typename TKeyTraits<TKey>::TAtomicKey TAtomicKey;
        typedef typename TOwner::TValue TValue;
        typedef typename TValueTraits<TValue>::TAtomicValue TAtomicValue;

        typedef TEntry<TKey, TValue> TEntryT;
        typedef TTable<TOwner> TTableT;
        typedef TConstIterator<TOwner> TConstIteratorT; 
        typedef typename TOwner::TPutCondition TPutCondition;
        typedef typename TOwner::TSearchHint TSearchHint;

        enum EResult {
            FULL_TABLE,
            SUCCEEDED,
            FAILED,

            RETRY,
            CONTINUE
        };

    public:
        TTable(TOwner* parent, size_t size)
            : Size(size)
            , MaxProbeCnt( Min(size_t(logf(size)) + 1, size - 1) )
            , IsFullFlag(false)
            , CopiedCnt(0)
            , CopyTaskSize(0)
            , Parent(parent)
            , Next(0)
            , NextToDelete(0)
        {
            Data.resize(Size);
        }

        ~TTable() {
            for (size_t i = 0; i < Data.size(); i++) {
                if (!ValueIsCopied((TValue)Data[i].Value))
                    UnRefKey((TKey)Data[i].Key);
                UnRefValue(PureValue((TValue)Data[i].Value));
            }
        }

        bool IsFull() const {
            return IsFullFlag;
        }

        TTableT* GetNext() {
            return Next;
        }

        void CreateNext();

        TEntryT* LookUp(const TKey& key, size_t hash, TKey& foundKey);
        void Copy(TEntryT* entry);
        
        bool GetEntry(TEntryT* entry, TValue& value);
        bool Get(const TKey& key, size_t hashValue, TValue& value, TSearchHint* hint);

        EResult FetchEntry(const TKey& key, TEntryT* entry, 
                           bool thereWasKey, bool& keyIsInstalled, const TPutCondition& cond);
        EResult PutEntry(TEntryT* entry, const TValue& value,
                         const TPutCondition& cond, bool updateAliveCnt);
        EResult Put(const TKey& key, const TValue& value, 
                    const TPutCondition& cond, bool& keyInstalled, bool updateAliveCnt = true);

        // JUST TO DEBUG 
        void Print(std::ostream& ostr);

    private:
        size_t Size;
        Atomic MaxProbeCnt;
        volatile bool IsFullFlag;
        
        Atomic CopiedCnt;
        size_t CopyTaskSize;

        std::vector<TEntryT> Data;

        TOwner* Parent;
        TTableT *volatile Next;
        TTableT *volatile NextToDelete;

        SpinLock Lock;

    private:
        void PrepareToDelete();
        void DoCopyTask();

        inline static TKey NoneKey() {
            return TKeyTraits<TKey>::None();
        }
        void UnRefKey(const TKey& key, size_t cnt = 1) {
            Parent->KeyTraits.UnRef(key, cnt);
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

        inline bool KeyIsNone(const TKey& key) {
            return KeysAreEqual(key, NoneKey());
        }
        inline bool ValueIsNone(const TValue& value) {
            return ValuesAreEqual(value, NoneValue());
        }
        inline bool ValueIsDeleted(const TValue& value) {
            return ValuesAreEqual(value, DeletedValue());
        }
        inline bool ValueIsBaby(const TValue& value) {
            return ValuesAreEqual(value, BabyValue());
        }
        inline bool ValueIsCopied(const TValue& value) {
            return ValuesAreEqual(value, CopiedValue());
        }

        inline bool KeysAreEqual(const TKey& lft, const TKey& rgh) {
            return Parent->KeysAreEqual(lft, rgh);
        }
        inline bool ValuesAreEqual(const TValue& lft, const TValue& rgh) {
            return Parent->ValuesAreEqual(lft, rgh);
        }

        inline static bool IsCopying(const TValue& value) {
            return TValueTraits<TValue>::IsCopying(value);
        }
        inline static void SetCopying(TAtomicValue& value) {
            TValueTraits<TValue>::SetCopying(value);
        }
        inline static TValue PureValue(const TValue& value) {
            return TValueTraits<TValue>::PureValue(value);
        }

        inline bool KeysCompareAndSet(TAtomicKey& key, const TKey& newKey, const TKey& oldKey) {
            return Parent->KeyTraits.CompareAndSet(key, newKey, oldKey);
        }
        inline bool ValuesCompareAndSet(TAtomicValue& value, const TValue& newValue, const TValue& oldValue) {
            return Parent->ValueTraits.CompareAndSet(value, newValue, oldValue);
        }

        inline void ReadValueAndRef(TValue& value, const TAtomicValue& atomicValue) {
            Parent->ValueTraits.ReadAndRef(value, atomicValue);
        }
        inline void UnRefValue(const TValue& value, size_t cnt = 1) {
            Parent->ValueTraits.UnRef(value, cnt);
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
    template <class Owner>
    class TConstIterator {
    public:
        friend class Owner::TSelf; 

        typedef typename Owner::TSelf TOwner;

        typedef typename TOwner::TKey TKey;
        typedef typename TOwner::TValue TValue;

        typedef TTable<TOwner> TTableT;
        typedef TEntry<TKey, TValue> TEntryT;
        typedef TConstIterator<TOwner> TConstIteratorT; 

        TKey Key() {
            return Table->Data[Index].Key;
        }

        TValue Value() {
            return Table->Data[Index].Value;
        }

        TConstIteratorT& operator ++ (int) {
            NextEntry();
            return *this;
        }
        TConstIteratorT& operator ++ () {
            NextEntry();
            return *this;
        }

        bool IsValid() { return Table; }
        
        TConstIterator(const TConstIterator& it)
            : Table(it.Table)
            , Index(it.Index)
        {
        }

    private:
        TTableT* Table;
        size_t Index;

    protected:
        void NextEntry();

    private:
        TConstIterator(TTableT* table, size_t index) :
            Table(table),
            Index(index)
        {
        }
   
        // traits wrappers
        bool KeysAreEqual(const TKey& lft, const TKey& rgh) {
            return Table->KeysAreEqual(lft, rgh);
        }
        bool ValuesAreEqual(const TValue& lft, const TValue& rgh) {
            return Table->ValuesAreEqual(lft, rgh);
        }
    };

    template <class Owner>
    typename TTable<Owner>::TEntryT*
    TTable<Owner>::LookUp(const TKey& key, size_t hash, TKey& foundKey) {
        assert(!KeyIsNone(key));
        VERIFY(Size, "Size must be non-zero\n");
        OnLookUp();

        size_t i = hash % Size;
        size_t probeCnt = 0;

        foundKey = NoneKey();
        TEntryT* returnEntry = 0;
        do {
            TKey entryKey(Data[i].Key);

            if (KeyIsNone(entryKey)) {
                returnEntry = &Data[i];
                break;
            }

            if (KeysAreEqual(entryKey, key)) {
                foundKey = key;
                returnEntry = &Data[i];
                break;
            }

            ++i;
            if (i == Size)
                i = 0;
            ++probeCnt;
        }
        while (probeCnt < Size);

        AtomicBase oldCnt;
        if (!IsFullFlag && probeCnt > MaxProbeCnt) {
            size_t keysCnt = Parent->GuardManager.TotalKeyCnt();

            double tooBigDensity = Min(0.7, 2 * Parent->Density);
            size_t tooManyKeys = Min(Size, (size_t)(ceil(tooBigDensity * Size)));
            // keysCnt is approximate, that's why we must check that table is absolutely full
            if (keysCnt >= tooManyKeys) {
                IsFullFlag = true;
            }
        }
        // cause TotalKeyCnt is approximate, sometimes table be absotely full, even 
        // when previous check fails
        if (!returnEntry && !IsFullFlag)
            IsFullFlag = true;
        
        return returnEntry;
    }

    // try to take value from entry
    // return false, if entry was copied
    template <class Owner>
    inline bool TTable<Owner>::GetEntry(TEntryT* entry, TValue& value) {
        if (IsCopying(TValue(entry->Value)))
            Copy(entry);
        ReadValueAndRef(value, entry->Value);
        bool canBeInNextTables = ValueIsCopied(value) || ValueIsDeleted(value);
        return !canBeInNextTables;
    }

    // tries to take value corresponding to key from table
    // returns false, if key information was copied
    template <class Owner> 
    bool TTable<Owner>::Get(const TKey& key, size_t hashValue, TValue& value, TSearchHint* hint) {
        TKey foundKey;
        TEntryT* entry = LookUp(key, hashValue, foundKey);
       
        // remember current head number before checking copy state
        // TAtomicBase tableNumber = Parent->TableNumber;

        bool result;
        bool keySet = !KeyIsNone(foundKey);
        if (keySet)
        {
            result = GetEntry(entry, value);
        }
        else
        {
            // if table is full we should continue search
            value = NoneValue();
            result = !IsFull();
            // VERIFY(!(!hint && result), "Table %zd nas podvela for key %zd\n", (size_t)this, (size_t)key);
        }

        return result;
    }
    
    template <class Owner>
    void TTable<Owner>::CreateNext() {
        VERIFY(IsFull(), "CreateNext in table when previous table is not full");

        Lock.Acquire();
        if (Next) {
            Lock.Release();
            return;
        }

        const size_t aliveCnt = Max(Max((AtomicBase)1, Parent->GuardManager.TotalAliveCnt()), (AtomicBase)Size);
        const size_t nextSize = Max((size_t)1, (size_t)ceil(aliveCnt * (1. / Parent->Density)));
        ZeroKeyCnt();

        Next = new TTableT(Parent, nextSize);
        CopyTaskSize = Max((size_t)1, 2 * (Size / (size_t)(Parent->Density * Next->Size + 1)));
        // Parent->GuardManager.ResetCounters();

        Lock.Release();
    }

    template <class Owner>
    void TTable<Owner>::Copy(TEntry<TKey, TValue>* entry) {
        OnCopy();

        SetCopying(entry->Value);
        // by now entry is locked for modifications (except becoming TOMBSTONE)
        // cause nobody does CAS on copying values

        // remember the value to copy to the next table
        TValue entryValue(PureValue((TValue)entry->Value));

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
        while (!ValueIsCopied(PureValue((TValue)entry->Value))) {
            if (!current->Next)
                current->CreateNext();
            TTableT* target = current->Next;

            bool tmp;
            if (target->Put(entryKey, entryValue, 
                            TPutCondition(TPutCondition::IF_ABSENT, BabyValue()), tmp, false) != FULL_TABLE)
                entry->Value = CopiedValue();
            else
                current = target;
        }
    }

    template <class Owner>
    typename TTable<Owner>::EResult
    TTable<Owner>::PutEntry(TEntryT* entry, const TValue& value, const TPutCondition& cond, bool updateCnt) {
        if (IsCopying((TValue)entry->Value)) {
            Copy(entry);
            return FULL_TABLE;
        }

        const bool shouldRefWhenRead = cond.When == TPutCondition::IF_MATCHES;
        const size_t successRefCnt = shouldRefWhenRead ? 2 : 1;
        const size_t otherRefCnt = successRefCnt - 1;
        
        bool shouldCompare = cond.When == TPutCondition::IF_MATCHES ||
                             cond.When == TPutCondition::IF_ABSENT;

        TValue oldValue;
        if (shouldRefWhenRead) {
            // we want to compare with oldValue
            // we need guaranty, that it's not deleted
            ReadValueAndRef(oldValue, entry->Value);
        } else {
            oldValue = PureValue((TValue)entry->Value);
        }
        if (ValueIsDeleted(oldValue) || ValueIsCopied(oldValue)) {
            UnRefValue(oldValue, otherRefCnt);
            return FULL_TABLE;
        }

        if (shouldCompare && !ValuesAreEqual(oldValue, cond.Value)) {
            UnRefValue(oldValue, otherRefCnt);
            return FAILED;
        }
        if (cond.When == TPutCondition::IF_EXISTS && ValueIsNone(oldValue))
            return FAILED;

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

    template <class Owner>
    typename TTable<Owner>::EResult
    TTable<Owner>::FetchEntry(const TKey& key, TEntryT* entry, 
                             bool thereWasKey, bool& keyIsInstalled, const TPutCondition& cond) {
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
        keyIsInstalled = false;
        if (KeyIsNone(entryKey)) {
            if (cond.When == TPutCondition::IF_EXISTS ||
                cond.When == TPutCondition::IF_MATCHES)
                return FAILED;
            if (!KeysCompareAndSet(entry->Key, key, NoneKey())) {
                return RETRY;
            }

            keyIsInstalled = true;
            IncreaseKeyCnt();
            return CONTINUE;
        }

        // key was not NONE, if it's not our key, caller should retry fetching
        if (!KeysAreEqual(entryKey, key))
            return RETRY;

        return CONTINUE;
    }

    template <class Owner>
    typename TTable<Owner>::EResult 
    TTable<Owner>::Put(const TKey& key, const TValue& value, const TPutCondition& cond, 
                      bool& keyInstalled, bool updateAliveCnt) {
        OnPut();

        const size_t hashValue = Parent->Hash(key);
        EResult result = RETRY;

        TEntryT* entry = 0;
        for (size_t cnt = 0; result == RETRY; ++cnt)
        {
            TKey foundKey;
            entry = LookUp(key, hashValue, foundKey);
            result = FetchEntry(key, entry, !KeyIsNone(foundKey), keyInstalled, cond);
            if (cnt == 10)
                VERIFY(false, "Fetch hang up");
        }
        if (result != CONTINUE)
            return result;

        for (size_t cnt = 0; (result = PutEntry(entry, value, cond, updateAliveCnt)) == RETRY; ++cnt) {
            if (cnt == 10)
                VERIFY(false, "Put hang up");
        }
        return result;
    }

    template <class Owner>
    void TTable<Owner>::DoCopyTask() {
        if (Parent->Head != this)
            return;
        if ((size_t)CopiedCnt >= Size) {
            if (CanPrepareToDelete())
                PrepareToDelete();
            return;
        }

        // try to set lock on throwing table away
        ForbidPrepareToDelete();

        // if table is already thrown away your lock is mistake
        if (Parent->Head != this) {
            AllowPrepareToDelete();
            return;
        }

        // help humanity to copy this fucking table
        size_t finish = AtomicAdd(CopiedCnt, CopyTaskSize);
        size_t start = finish - CopyTaskSize;
        if (start < Size) {
            finish = std::min(Size, finish);
            for (size_t i = start; i < finish; i++)
                Copy(&Data[i]);
        }

        // your job is done 
        AllowPrepareToDelete();
        // check, maybe it's time to throw away table
        if ((size_t)CopiedCnt >= Size && CanPrepareToDelete()) 
            PrepareToDelete();
    }

    template <class Owner>
    void TTable<Owner>::PrepareToDelete() {
        AtomicBase currentTableNumber = Parent->TableNumber;
        if (Parent->Head == this && AtomicCas(&Parent->Head, Next, this)) {
            // deleted table from main list
            // now it's only thread that has pointer to it
            AtomicIncrement(Parent->TableNumber);
            Parent->TableToDeleteNumber = currentTableNumber;
            while (true) {
                TTable* toDelete = Parent->ToDelete;
                NextToDelete = toDelete;
                if (AtomicCas(&Parent->ToDelete, this, toDelete)) {
                    break;
                }
            }
        }
    }

    template <class Owner>
    void TConstIterator<Owner>::NextEntry() {
        ++Index;

        while (Table) {
            for (; Index < Table->Size; Index++) {
                // it's rather fast to copy small entry
                const TEntryT& e = Table->Data[Index];
                if (!KeysAreEqual((TKey)e.Key, TKeyTraits<TKey>::None()) &&
                    !TValueTraits<TValue>::IsCopying((TValue)e.Value) &&
                    !ValuesAreEqual((TValue)e.Value, TValueTraits<TValue>::Baby()) && 
                    !ValuesAreEqual((TValue)e.Value, TValueTraits<TValue>::None()) && 
                    !ValuesAreEqual((TValue)e.Value, TValueTraits<TValue>::Copied()) &&
                    !ValuesAreEqual((TValue)e.Value, TValueTraits<TValue>::Deleted()))
                    break;
            }
            if (Index < Table->Size)
                break;
            Index = 0;
            Table = Table->Next;
        }
    }

    // JUST TO DEBUG

    template <class Owner>
    void TTable<Owner>::Print(std::ostream& ostr) {
        std::stringstream buf;

        buf << "Table at " << (size_t)this << ":\n";

        buf << "Size: " << Size << '\n'
            << "CopiedCnt: " << CopiedCnt << '\n'
            << "CopyTaskSize: " << CopyTaskSize << '\n';

        for (size_t i = 0; i < Size; i++) 
            buf << "Entry " << i << ": "
                << "(" << KeyToString<TKey>((TKey)Data[i].Key)
                << "; " << ValueToString<TValue>((TValue)Data[i].Value)  
                << ")\n";

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
