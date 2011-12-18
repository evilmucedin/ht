#pragma once

#include "atomic_traits.h"

#include <cerrno>
#include <vector>
#include <stdarg.h>

#include "lfht.h"

template <class K, class V>
class TLFHashTable;

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

    template <class K, class V>
    class TConstIterator;

    template <class K, class V>
    class TTable {
    private:
        typedef TEntry<K, V> TEntryT;
        typedef TTable<K, V> TTableT;
        typedef TLFHashTable<K, V> TLFHashTableT;
        typedef TConstIterator<K, V> TConstIteratorT; 
        typedef typename TLFHashTableT::TPutCondition TPutCondition;
        typedef typename TLFHashTableT::TSearchHint TSearchHint;

        friend class TLFHashTable<K, V>;
        friend class TConstIterator<K, V>;

        typedef typename TKeyTraits<K>::TKey TKey;
        typedef typename TKeyTraits<K>::TAtomicKey TAtomicKey;

        typedef typename TValueTraits<V>::TValue TValue;
        typedef typename TValueTraits<V>::TAtomicValue TAtomicValue;

        size_t Size;
        Atomic MaxProbeCnt;
        volatile bool IsFullFlag;
        
        Atomic CopiedCnt;
        size_t CopyTaskSize;

        std::vector<TEntryT> Data;

        TLFHashTableT* Parent;
        TTableT *volatile Next;
        TTableT *volatile NextToDelete;

        SpinLock Lock;

    public:

        enum EResult {
            FULL_TABLE,
            SUCCEEDED, 
            FAILED,

            RETRY,
            CONTINUE
        };

    public:      
        TTable(TLFHashTableT* parent, size_t size) :
            Size(size),
            MaxProbeCnt(0), 
            IsFullFlag(false),
            CopiedCnt(0),
            CopyTaskSize(0),
            Parent(parent),
            Next(0),
            NextToDelete(0)
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

        bool IsFull() {
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
        void PrepareToDelete();    
        void DoCopyTask();

        // traits wrappers
        static TKey NoneKey() {
            return TKeyTraits<K>::None();
        }
        void UnRefKey(const TKey& key, size_t cnt = 1) {
            Parent->KeyTraits.UnRef(key, cnt);
        }

        static TValue CopiedValue() {
            return TValueTraits<V>::Copied();
        }
        static TValue NoneValue() {
            return TValueTraits<V>::None();
        }
        static TValue DeletedValue() {
            return TValueTraits<V>::Deleted();
        }
        static TValue BabyValue() {
            return TValueTraits<V>::Baby();
        }

        bool KeyIsNone(const TKey& key) {
            return KeysAreEqual(key, NoneKey());
        }
        bool ValueIsNone(const TValue& value) {
            return ValuesAreEqual(value, NoneValue());
        }
        bool ValueIsDeleted(const TValue& value) {
            return ValuesAreEqual(value, DeletedValue());
        }
        bool ValueIsBaby(const TValue& value) {
            return ValuesAreEqual(value, BabyValue());
        }
        bool ValueIsCopied(const TValue& value) {
            return ValuesAreEqual(value, CopiedValue());
        }

        bool KeysAreEqual(const TKey& lft, const TKey& rgh) {
            return Parent->KeysAreEqual(lft, rgh);
        }
        bool ValuesAreEqual(const TValue& lft, const TValue& rgh) {
            return Parent->ValuesAreEqual(lft, rgh);
        }

        static bool IsCopying(const TValue& value) {
            return TValueTraits<V>::IsCopying(value);
        }
        static void SetCopying(TAtomicValue& value) {
            TValueTraits<V>::SetCopying(value);
        }
        static TValue PureValue(const TValue& value) {
            return TValueTraits<V>::PureValue(value);
        }

        bool KeysCompareAndSet(TAtomicKey& key, const TKey& newKey, const TKey& oldKey) {
            return Parent->KeyTraits.CompareAndSet(key, newKey, oldKey);
        }
        bool ValuesCompareAndSet(TAtomicValue& value, const TValue& newValue, const TValue& oldValue) {
            return Parent->ValueTraits.CompareAndSet(value, newValue, oldValue);
        }

        void ReadValueAndRef(TValue& value, const TAtomicValue& atomicValue) {
            Parent->ValueTraits.ReadAndRef(value, atomicValue);
        }
        void UnRefValue(const TValue& value, size_t cnt = 1) {
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
    template <class K, class V>
    class TConstIterator {
    private:
        friend class TLFHashTable<K, V>;

        typedef TTable<K, V> TTableT;
        typedef TEntry<K, V> TEntryT;

        TTableT* Table;
        size_t Index;
    public:
        typedef typename TKeyTraits<K>::TKey TKey;
        typedef typename TValueTraits<V>::TValue TValue;

        typedef TLFHashTable<K, V> TLFHashTableT;
        typedef TConstIterator<K, V> TConstIteratorT; 

        TKey Key() { return Table->Data[Index].Key; } 
        TValue Value() { return Table->Data[Index].Value; }     

        TConstIteratorT& operator ++ (int) {
            NextEntry();
            return *this;
        }
        TConstIteratorT& operator ++ () {
            NextEntry();
            return *this;
        }

        bool IsValid() { return Table; }
    protected:
        TConstIterator(const TConstIterator& it) :
            Table(it.Table),
            Index(it.Index)
        {
        }
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

    template <class K, class V>
    TEntry<K, V>* TTable<K, V>::LookUp(const TKey& key, size_t hash, TKey& foundKey) {
        assert(!KeyIsNone(key));
        VERIFY(Size, "Size must be non-zero\n");
        OnLookUp();

#ifdef TRACE
        Trace(Cerr, "LookUp for key \"%s\"\n", ~KeyToString<K>(key));
#endif
        size_t i = hash % Size; 
        size_t probeCnt = 0; 

#ifdef TRACE
        Trace(Cerr, "Start from entry %zd\n", i);
#endif
        foundKey = NoneKey(); 
        TEntryT* returnEntry = 0;
        do {
            TKey entryKey(Data[i].Key);

            if (KeyIsNone(entryKey)) {
#ifdef TRACE
                Trace(Cerr, "Found empty entry %zd\n", i);
#endif
                returnEntry = &Data[i];
                break;
            }
            if (KeysAreEqual(entryKey, key)) {
#ifdef TRACE
                Trace(Cerr, "Found key\n");
#endif
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
        while (!IsFullFlag && probeCnt > (size_t)(oldCnt = MaxProbeCnt))
            if (AtomicCas(&MaxProbeCnt, probeCnt, oldCnt)) {
                size_t keysCnt = Parent->GuardManager.TotalKeyCnt();
                size_t tooManyKeys = Min(Size, (size_t)(ceil(2 * Parent->Density * Size)));
#ifdef TRACE
                Trace(Cerr, "MaxProbeCnt now %zd, keysCnt now %zd, tooManyKeys now %zd\n",
                            probeCnt, keysCnt, tooManyKeys);
#endif
                // keysCnt is approximate, that's why we must check that table is absolutely full
                if (keysCnt >= tooManyKeys) {
#ifdef TRACE_MEM
                    Trace(Cerr, "Claim that table is full\n");
#endif
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
    template <class K, class V> 
    inline bool TTable<K, V>::GetEntry(TEntryT* entry, TValue& value) {
#ifdef TRACE
        Trace(Cerr, "GetEntry in %zd\n", entry - &Data[0]); 
#endif
        if (IsCopying(TValue(entry->Value)))
            Copy(entry);
        ReadValueAndRef(value, entry->Value);
        bool canBeInNextTables = ValueIsCopied(value) || ValueIsDeleted(value);
        return !canBeInNextTables;
    }    

    // tries to take value corresponding to key from table
    // returns false, if key information was copied
    template <class K, class V> 
    bool TTable<K, V>::Get(const TKey& key, size_t hashValue, TValue& value, TSearchHint* hint) {
        TKey foundKey;
        TEntryT* entry = LookUp(key, hashValue, foundKey);
       
        // remember current head number before checking copy state
        // TAtomicBase tableNumber = Parent->TableNumber;

        bool result;
        bool keySet = !KeyIsNone(foundKey);
        if (keySet) {
            result = GetEntry(entry, value);     
        }
        else {
            // if table is full we should continue search
            value = DeletedValue();
            result = !IsFull();
            // VERIFY(!(!hint && result), "Table %zd nas podvela for key %zd\n", (size_t)this, (size_t)key);
        }

        return result;
    }        
    
    template <class K, class V>
    void TTable<K, V>::CreateNext() {
        VERIFY(IsFull(), "CreateNext in table when previous table is not full");

        Lock.Acquire();
        if (Next) {
            Lock.Release();
            return;
        }
#ifdef TRACE
        Trace(Cerr, "CreateNext\n");
#endif

        const size_t aliveCnt = Max(Max((AtomicBase)1, Parent->GuardManager.TotalAliveCnt()), (AtomicBase)Size);
        const size_t nextSize = Max((size_t)1, (size_t)ceil(aliveCnt * (1. / Parent->Density)));
        ZeroKeyCnt();

        Next = new TTableT(Parent, nextSize);
#ifdef TRACE
        Trace(Cerr, "Table done\n");
#endif
        CopyTaskSize = Max((size_t)1, 2 * (Size / (size_t)(Parent->Density * Next->Size + 1)));
        // Parent->GuardManager.ResetCounters();
#ifdef TRACE_MEM
        Trace(Cerr, "AliveCnt %zd\n", aliveCnt);
        Trace(Cerr, "New table %zd of size %zd\n", Next, Next->Size);
#endif

        Lock.Release();
    }

    template <class K, class V>
    void TTable<K, V>::Copy(TEntry<K, V>* entry) {
        OnCopy();

        SetCopying(entry->Value);
        // by now entry is locked for modifications (except becoming TOMBSTONE)
        // cause nobody does CAS on copying values

#ifdef TRACE
        Trace(Cerr, "Copy \"%s\"\n", ~KeyToString<K>(TKey(entry->Key)));
#endif
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

    template <class K, class V>
    typename TTable<K, V>::EResult
    TTable<K, V>::PutEntry(TEntryT* entry, const TValue& value, const TPutCondition& cond, bool updateCnt) {
#ifdef TRACE
        Trace(Cerr, "PutEntry in entry %zd value %s under condition %s\n", entry - &Data[0], 
                     ~ValueToString<V>(value), ~cond.ToString()); 
#endif

        if (IsCopying((TValue)entry->Value)) {
            Copy(entry);
            return FULL_TABLE;
        }

        bool shouldRefWhenRead = cond.When == TPutCondition::IF_MATCHES;
        size_t successRefCnt = shouldRefWhenRead ? 2 : 1;
        size_t otherRefCnt = successRefCnt - 1;
        
        bool shouldCompare = cond.When == TPutCondition::IF_MATCHES ||
                             cond.When == TPutCondition::IF_ABSENT;

        TValue oldValue;
        if (shouldRefWhenRead) 
            // we want to compare with oldValue
            // we need guaranty, that it's not deleted
            ReadValueAndRef(oldValue, entry->Value);
        else
            oldValue = PureValue((TValue)entry->Value);
        if (ValueIsDeleted(oldValue) || ValueIsCopied(oldValue)) {
            UnRefValue(oldValue, otherRefCnt);
            return FULL_TABLE;
        }

        if (shouldCompare && !ValuesAreEqual(oldValue, cond.Value)) {
            UnRefValue(oldValue, otherRefCnt);
            return FAILED;
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

    template <class K, class V>
    typename TTable<K, V>::EResult
    TTable<K, V>::FetchEntry(const TKey& key, TEntryT* entry, 
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
#ifdef TRACE
                Trace(Cerr, "Lost race for instaling key\n");
#endif
                return RETRY;
            }

            keyIsInstalled = true;
            IncreaseKeyCnt();
#ifdef TRACE
            Trace(Cerr, "Key installed\n");
#endif
            return CONTINUE;
        }

        // key was not NONE, if it's not our key, caller should retry fetching
        if (!KeysAreEqual(entryKey, key))
            return RETRY;

        return CONTINUE;
    }        

    template <class K, class V>
    typename TTable<K, V>::EResult 
    TTable<K, V>::Put(const TKey& key, const TValue& value, const TPutCondition& cond, 
                      bool& keyInstalled, bool updateAliveCnt) {
        OnPut();
#ifdef TRACE
        Trace(Cerr, "Put key \"%s\" and value \"%s\" under condition %s..\n",
                          ~KeyToString<K>(key), 
                          ~ValueToString<V>(value), 
                          ~cond.ToString());
#endif

        size_t hashValue = Parent->Hash(key);
        EResult result = RETRY;

        TEntryT* entry = 0;
        for (size_t cnt = 0; result == RETRY; cnt++)
        {
            TKey foundKey;
            entry = LookUp(key, hashValue, foundKey);
            result = FetchEntry(key, entry, !KeyIsNone(foundKey), keyInstalled, cond);
            if (cnt == 10)
                VERIFY(false, "Fetch hang up");
        }
        if (result != CONTINUE)
            return result;

#ifdef TRACE
        Trace(Cerr, "Got entry %d\n", entry - &Data[0]);
#endif
        for (size_t cnt = 0; (result = PutEntry(entry, value, cond, updateAliveCnt)) == RETRY; cnt++) {
            if (cnt == 10) 
                VERIFY(false, "Put hang up %s %s %s\n");
        }
        return result;
    }

    template <class K, class V>
    void TTable<K, V>::DoCopyTask() {
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
#ifdef TRACE
            Trace(Cerr, "Copy from %d to %d\n", start, finish); 
#endif        
            for (size_t i = start; i < finish; i++)
                Copy(&Data[i]);   
        }

        // your job is done 
        AllowPrepareToDelete();
        // check, maybe it's time to throw away table
        if ((size_t)CopiedCnt >= Size && CanPrepareToDelete()) 
            PrepareToDelete();
    }

    template <class K, class V>
    void TTable<K, V>::PrepareToDelete() {
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

    template <class K, class V>
    void TConstIterator<K, V>::NextEntry() {
        Index++;

        while (Table) {
            for (; Index < Table->Size; Index++) {
                // it's rather fast to copy small entry
                const TEntryT& e = Table->Data[Index];
                if (!KeysAreEqual((TKey)e.Key, TKeyTraits<K>::None()) &&
                    !TValueTraits<V>::IsCopying((TValue)e.Value) &&
                    !ValuesAreEqual((TValue)e.Value, TValueTraits<V>::Copied()) &&
                    !ValuesAreEqual((TValue)e.Value, TValueTraits<V>::Baby()) && 
                    !ValuesAreEqual((TValue)e.Value, TValueTraits<V>::Deleted()))
                    break;
            }
            if (Index < Table->Size)
                break;
            Index = 0;
            Table = Table->Next;
        }
    }

    // JUST TO DEBUG

    template <class K, class V>
    void TTable<K, V>::Print(std::ostream& ostr) {
        std::stringstream buf;

        buf << "Table at " << (size_t)this << ":\n";

        buf << "Size: " << Size << '\n'
            << "CopiedCnt: " << CopiedCnt << '\n'
            << "CopyTaskSize: " << CopyTaskSize << '\n';

        for (size_t i = 0; i < Size; i++) 
            buf << "Entry " << i << ": "
                << "(" << KeyToString<K>((TKey)Data[i].Key)
                << "; " << ValueToString<V>((TValue)Data[i].Value)  
                << ")\n";

        ostr << buf.str();
    }
   
    template <class K, class V>
    void TTable<K, V>::Trace(std::ostream& ostr, const char* format, ...) {
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
