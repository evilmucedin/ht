#pragma once

#include <iostream>

#include "atomic.h"
#include "unordered_map"

#include "transp_holder.h"

namespace NLFHT {
    class TBaseGuard;

    class TGuardable {
    public:
        virtual TBaseGuard* AcquireGuard() = 0;
    };

    class TBaseGuardManager;

    class TBaseGuard : NonCopyable
    {
    public:
        friend class TBaseGuardManager;
        friend class TThreadGuardTable;

    public:
        TBaseGuard(TBaseGuardManager* parent);
        virtual ~TBaseGuard();

        void Release();
        TBaseGuard* GetNext() {
            return Next;
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
#ifndef NDEBUG
        inline void OnLocalPut() {
            ++LocalPutCnt;
        }
        inline void OnLocalDelete() {
            ++LocalDeleteCnt;
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
        virtual std::string ToString();

        size_t GetThreadId() const {
            return ThreadId;
        }

    private:
        void Init();

    private:
        static const AtomicBase NO_TABLE;

        TBaseGuard* Next;
        TBaseGuardManager* Parent;

        volatile size_t GuardedTable;
        volatile bool PTDLock;
        // to exclude probability, that data from different
        // tables are in the same cache line
        char Padding[CACHE_LINE_SIZE];

#ifndef NDEBUG
        // JUST TO DEBUG
        Atomic LocalPutCnt, LocalCopyCnt, LocalDeleteCnt, LocalLookUpCnt;
        Atomic GlobalPutCnt, GlobalGetCnt;
#endif

        Atomic AliveCnt;
        Atomic KeyCnt;

        volatile size_t ThreadId;
    };

    class TThreadGuardTable : NonCopyable {
    public:
        static void RegisterTable(TGuardable* pTable);
        static void ForgetTable(TGuardable* pTable);

        static TBaseGuard* ForTable(TGuardable* pTable) {
            assert(GuardTable);
            TGuardTable::const_iterator toTable = GuardTable->find(pTable);
            assert(toTable != GuardTable->end());
            return toTable->second;
        }
    private:
        // yhash_map has too big constant
        // more specialized hash_map should be used here
        typedef std::unordered_map<TGuardable*, TBaseGuard*> TGuardTable;
        static NLFHT_THREAD_LOCAL TGuardTable* GuardTable;
    };

    class TBaseGuardManager {
    public:
        friend class TBaseGuard;

        TBaseGuardManager();

        TBaseGuard* GetHead() {
            return Head;
        }
        TBaseGuard* AcquireGuard();

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
        class THeadHolder : public TVolatilePointerWrapper<TBaseGuard> {
        public:
            THeadHolder(TBaseGuardManager* parent)
                : Parent(parent)
            {
            }
            inline THeadHolder& operator= (TBaseGuard* ptr) {
                Set(ptr);
                return *this;
            }

            ~THeadHolder() {
                TBaseGuard* current = Get();
                while (current) {
                    TBaseGuard* tmp = current;
                    current = current->Next;
                    delete tmp;
                }
#ifndef NDEBUG
                if (Parent->GuardsCreated != Parent->GuardsDeleted) {
                    std::cerr << "GuardsCreated " << Parent->GuardsCreated << '\n'
                         << "GuardsDeleted " << Parent->GuardsDeleted << '\n';
                    assert(false && !"Some guard lost");
                }
#endif
            }
        private:
            TBaseGuardManager* Parent;
        };

        THeadHolder Head;

        Atomic AliveCnt;
        Atomic KeyCnt;

#ifndef NDEBUG
        Atomic GuardsCreated;
        Atomic GuardsDeleted;
#endif

    private:
        TBaseGuard* CreateGuard();
        virtual TBaseGuard* NewGuard() {
            return new TBaseGuard(this);
        }
    };

    template <class Prt>
    class TGuard : public TBaseGuard {
    public:
        typedef Prt TParent;
        typedef typename TParent::TGuardManager TGuardManager;

        TGuard(TGuardManager* parent)
            : TBaseGuard(parent)
        {
        }
    };

    template <class Prt>
    class TGuardManager : public TBaseGuardManager {
    public:
        typedef Prt TParent;
        typedef typename TParent::TGuard TGuard;

        TGuardManager(TParent* parent)
            : Parent(parent)
        {
        }
    private:
        TParent* Parent;
    private:
        virtual TBaseGuard* NewGuard() {
            return new TGuard(this);
        }
    };
}
