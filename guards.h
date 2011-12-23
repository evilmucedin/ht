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
        
#ifndef NDEBUG
        inline void OnLocalPut() {
            ++LocalPutCnt;
        }
        inline void OnLocalDelete() {
            LocalDeleteCnt;
        }
        inline void OnLocalLookUp() {
            ++LocalLookUpCnt;
        }
        inline void OnLocalCopy() {
            ++LocalCopyCnt;
        }
        inline void OnGlobalGet() {
            ++GlobalGetCnt;
        }
        inline void OnGlobalPut() {
            ++GlobalPutCnt;
        }
#else
        inline void OnLocalPut() {
        }
        inline void OnLocalDelete() {
        }
        inline void OnLocalLookUp() {
        }
        inline void OnLocalCopy() {
        }
        inline void OnGlobalGet() {
        }
        inline void OnGlobalPut() {
        }
#endif

        inline void IncreaseAliveCnt() {
            AtomicIncrement(AliveCnt);
        }
        inline void DecreaseAliveCnt() {
            AtomicDecrement(AliveCnt);
        }
        inline void IncreaseKeyCnt() {
            AtomicIncrement(KeyCnt);
        }

        // JUST TO DEBUG
        std::string ToString();
        
    private:
        void Init();

    private:
        static const AtomicBase NO_OWNER;
        static const AtomicBase NO_TABLE;

        TGuard* Next;
        TGuardManager* Parent;

        Atomic Owner;

        volatile size_t GuardedTable;
        volatile bool PTDLock;
        // to exclude probability, that data from different
        // tables are in the same cache line
        char Padding [CACHE_LINE_SIZE];

#ifndef NDEBUG
        // JUST TO DEBUG
        Atomic LocalPutCnt, LocalCopyCnt, LocalDeleteCnt, LocalLookUpCnt;
        Atomic GlobalPutCnt, GlobalGetCnt;
#endif

        Atomic AliveCnt;
        Atomic KeyCnt;
    };

    class TThreadGuardTable : NonCopyable {
    public:
        static void RegisterTable(TLFHashTableBase* pTable);
        static void ForgetTable(TLFHashTableBase* pTable);

        static TGuard* ForTable(TLFHashTableBase *pTable) {
            assert(GuardTable);
            return (*GuardTable)[pTable];
        }
    private:
        // yhash_map has too big constant
        // more specialized hash_map should be used here
        typedef std::unordered_map<TLFHashTableBase*, TGuard*> TGuardTable;
        static NLFHT_THREAD_LOCAL TGuardTable* GuardTable;
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
        AtomicBase TotalKeyCnt();
        void ZeroKeyCnt();

        bool CanPrepareToDelete();

        // JUST TO DEBUG
        void PrintStatistics(std::ostream& str);

        std::string ToString();

    private:
        TGuard *volatile Head;

        Atomic AliveCnt;
        Atomic KeyCnt;

    private:
        TGuard* CreateGuard(AtomicBase owner);
    };
};

