#pragma once

#include "atomic.h"
#include "unordered_map"

namespace NLFHT {
    class TLFHashTableBase;

    class TGuardManager;

    class TGuard {
    public:
        friend class TGuardManager;

    public:
        TGuard(TGuardManager* parent);

        void Release();

        void GuardTable(AtomicBase tableNumber) {
            GuardedTable = tableNumber;
        }
        void StopGuarding() {
            GuardedTable = NO_TABLE; 
        }

        void ForbidPrepareToDelete() {
            PTDLock = true;
        }    
        void AllowPrepareToDelete() {
            PTDLock = false;
        }

        // JUST TO DEBUG 
        
        void OnLocalPut() {
            LocalPutCnt++;
        }
        void OnLocalDelete() {
            LocalDeleteCnt++;
        }
        void OnLocalLookUp() {
            LocalLookUpCnt++;
        }
        void OnLocalCopy() {
            LocalCopyCnt++;
        }
        void OnGlobalGet() {
            GlobalGetCnt++;
        }
        void OnGlobalPut() {
            GlobalPutCnt++;
        }

        void IncreaseAliveCnt() {
            AtomicIncrement(AliveCnt);
        }
        void DecreaseAliveCnt() {
            AtomicDecrement(AliveCnt);
        }
        void IncreaseKeyCnt() {
            AtomicIncrement((Atomic&)KeyCnt);
        }

        // JUST TO DEBUG
        Stroka ToString();
        
    private:
        void Init();

    private:
        static const TAtomicBase NO_OWNER;
        static const TAtomicBase NO_TABLE;

        TGuard* Next;
        TGuardManager* Parent;

        TAtomic Owner;

        volatile size_t GuardedTable;
        volatile bool PTDLock;
        // to exclude probability, that data from different
        // tables are in the same cache line
        char Padding [CACHE_LINE_SIZE];

        // JUST TO DEBUG
        TAtomic LocalPutCnt, LocalCopyCnt, LocalDeleteCnt, LocalLookUpCnt;
        TAtomic GlobalPutCnt, GlobalGetCnt;

        TAtomic AliveCnt;
        TAtomic KeyCnt;
    };

    class TThreadGuardTable : TNonCopyable {
    public:
        static void RegisterTable(TLFHashTableBase* pTable);
        static void ForgetTable(TLFHashTableBase* pTable);

        static TGuard* ForTable(TLFHashTableBase *pTable) {
            YASSERT(GuardTable);
            return (*GuardTable)[pTable];
        }
    private:
        // yhash_map has too big constant
        // more specialized hash_map should be used here
        typedef yhash_map<TLFHashTableBase*, TGuard*> TGuardTable;
        POD_STATIC_THREAD(TGuardTable*) GuardTable;
    };

    class TGuardManager {
    public:
        friend class TGuard;

        TGuardManager();
        ~TGuardManager();        

        TGuard* AcquireGuard(size_t owner);

        size_t GetFirstGuardedTable();

        // returns approximate value
        AtomicBase TotalAliveCnt();

        // returns approximate value 
        TAtomicBase TotalKeyCnt();
        void ZeroKeyCnt();

        bool CanPrepareToDelete();

        // JUST TO DEBUG
        void PrintStatistics(TOutputStream& str);

        Stroka ToString();
    private:
        TGuard *volatile Head;

        TAtomic AliveCnt;
        TAtomic KeyCnt;

    private:
        TGuard* CreateGuard(AtomicBase owner);
    };
};

