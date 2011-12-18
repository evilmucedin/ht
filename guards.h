#pragma once

#include "atomic.h"
#include "unordered_map"

namespace NLFHT {
    class TGuard {
    private:
        friend class TGuardManager;

        static const AtomicBase NO_OWNER;
        static const AtomicBase NO_TABLE;

        TGuard* Next;    
        Atomic Owner;

        volatile size_t GuardedTable;
        volatile bool PTDLock;
        // to exclude probability, that data from different
        // tables are in the same cache line
        char Padding [CACHE_LINE_SIZE];

        // JUST TO DEBUG
        Atomic LocalPutCnt, LocalCopyCnt, LocalDeleteCnt, LocalLookUpCnt;
        Atomic GlobalPutCnt, GlobalGetCnt;

    public:
        TGuard();

        void Release() {
            Owner = NO_OWNER;
        }

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

    public:    
        Atomic AliveCnt;
        volatile size_t KeyCnt;
    };

    class TThreadGuardTable {
    public:
        static void InitializePerThread();
        static void FinalizePerThread();

        static TGuard*& ForTable(void *tableAddress) {
            assert(GuardTable);
            return (*GuardTable)[tableAddress];
        }
    private:        
        typedef std::unordered_map<void*, TGuard*> TGuardTable;
        static NLFHT_THREAD_LOCAL TGuardTable* GuardTable;        
    };

    class TGuardManager {
    private:
        TGuard *volatile Head;
    public:
        TGuardManager() :
            Head(0)
        {
        }

        TGuard* AcquireGuard(size_t owner);

        size_t GetFirstGuardedTable();

        // returns approximate value
        AtomicBase TotalAliveCnt();

        // returns approximate value 
        size_t TotalKeyCnt();
        void ZeroKeyCnt();

        bool CanPrepareToDelete();

        // JUST TO DEBUG
        void PrintStatistics(std::ostream& str);

    private:
        TGuard* CreateGuard(AtomicBase owner);
    };
};

